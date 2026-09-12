const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const WebSocket = require('ws');
const aedes = require('aedes');
const { v4: uuidv4 } = require('uuid');
const mqtt = require('mqtt');
const {
  normalizePayload,
  parsePipeKv,
  unwrapCommandEnvelope,
  parseTrafficReport,
  parseEventMessage,
} = require('./parsers.js');
const crypto = require('crypto');
const multer = require('multer');

const app = express();
const server = http.createServer(app);
const broker = aedes();
const wsServer = new WebSocket.Server({ server, path: '/ws' });
const mqttPort = process.env.MQTT_PORT || 1010;
const httpPort = process.env.PORT || 1011;
const dataDir = path.join(__dirname, 'data');
const devicesFile = path.join(dataDir, 'devices.json');
const firmwareDir = path.join(dataDir, 'firmware');

const state = {
  devices: [],
  clients: new Map(),
  messages: [],
  pendingCommands: new Map(), // cmd_id -> { boardId, command, ts }
  cmdCounter: 0,
  stats: {
    totalBoards: 0,
    onlineBoards: 0,
    totalVehicles: 0,
    avgSpeed: 0,
    violations: 0,
    events: []
  }
};

function defaultProfile() {
  return {
    name: 'New Board',
    loopPairs: [
      { enabled: true, distance_m: 2.2, sensor1: 0, ch1: 0, sensor2: 1, ch2: 0 },
      { enabled: false, distance_m: 2.4, sensor1: 0, ch1: 1, sensor2: 1, ch2: 1 }
    ],
    detector: {
      min_event_ms: 25,
      max_event_ms: 5000,
      enter_thresh: 1.5,
      exit_ratio: 0.7,
      auto_threshold: true,
      enter_sigma: 4,
      abs_sigma: 6
    },
    classification: {
      motor_max_len: 1.2,
      car_max_len: 3.8,
      pickup_max_len: 5.2,
      van_max_len: 6.5,
      bus_max_len: 10.5,
      truck_s_max_len: 12,
      truck_2_max_len: 15,
      truck_3_max_len: 18,
      truck_4_plus_min_len: 20
    },
    report: {
      enabled: true,
      interval_min: 1,
      clear_on_report: false
    }
  };
}

function ensureDataDir() {
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
  if (!fs.existsSync(firmwareDir)) fs.mkdirSync(firmwareDir, { recursive: true });
  if (!fs.existsSync(devicesFile)) {
    fs.writeFileSync(devicesFile, JSON.stringify({ devices: [] }, null, 2));
  }
}

function loadDevices() {
  ensureDataDir();
  try {
    const raw = fs.readFileSync(devicesFile, 'utf8');
    const parsed = JSON.parse(raw);
    state.devices = Array.isArray(parsed.devices) ? parsed.devices : [];
  } catch (err) {
    state.devices = [];
  }
  // Devices restored from disk are, by definition, not connected yet.
  // The dashboard must only mark them "online" when an actual MQTT
  // connection event or live message is observed.
  state.devices.forEach((d) => {
    d.status = 'offline';
    d.everConnected = !!d.everConnected;
  });
}

function saveDevices() {
  ensureDataDir();
  fs.writeFileSync(devicesFile, JSON.stringify({ devices: state.devices }, null, 2));
}

function recordMessage(kind, topic, payload, clientId = '') {
  const message = {
    kind,
    topic: String(topic || ''),
    payload: normalizePayload(payload),
    clientId: String(clientId || ''),
    ts: new Date().toISOString()
  };
  state.messages.push(message);
  if (state.messages.length > 500) state.messages = state.messages.slice(-500);

  const wireMessage = JSON.stringify({ type: 'message', message });
  wsServer.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) client.send(wireMessage);
  });
}

function findDevice(deviceId) {
  return state.devices.find((d) => d.id === deviceId);
}

function buildDeviceSummary(device) {
  const profile = device.profile || defaultProfile();
  return {
    id: device.id,
    name: device.name || profile.name,
    status: device.status,
    lastSeen: device.lastSeen,
    battery: device.power?.battery ?? null,
    solar: device.power?.solar ?? null,
    temp: device.power?.temp ?? null,
    signal: device.network?.signal ?? null,
    loopConfig: device.loopConfig ?? profile.loopPairs ?? null,
    classification: device.classification ?? profile.classification ?? null,
    profile,
    stats: device.stats ?? null,
    lastReply: device.lastReply ?? null,
    commandLog: Array.isArray(device.commandLog) ? device.commandLog.slice(-20) : [],
    trafficReports: Array.isArray(device.trafficReports) ? device.trafficReports : []
  };
}

// Strip the optional CMD|cmd_id| / RSP|cmd_id| envelope and return both the
// inner payload and the cmd_id (if present). The envelope is purely additive;
// commands without it are still treated as legacy/unaddressed messages.
//
// (parsePipeKv, parseTrafficReport, parseEventMessage and friends are imported
//  from ./parsers.js at the top of this file. The full parseEventMessage with
//  POWER / BATTERY_* / TRAFFIC_REPORT / STATUS branches lives in this file.)

function parseEventMessageLocal(deviceId, message) {
  const evt = { deviceId, raw: message, ts: new Date().toISOString() };
  if (message.startsWith('MQTT_ID|')) {
    const value = message.substring('MQTT_ID|'.length).trim();
    evt.type = 'identity';
    evt.boardId = value;
    evt.clientId = value;
    return evt;
  }
  if (message.startsWith('EVENT|')) {
    const kv = parsePipeKv(message.substring('EVENT|'.length));
    Object.assign(evt, kv);
    evt.type = 'event';
    return evt;
  }
  if (message.startsWith('SPEED|')) {
    const kv = parsePipeKv(message.substring('SPEED|'.length));
    Object.assign(evt, kv);
    evt.type = 'speed';
    evt.loopIndex = Number(kv.idx || 0);
    evt.speed_kmh = Number(kv.speed || 0);
    evt.length_m = Number(kv.len || 0);
    evt.delay_ms = Number(kv.delay || 0);
    evt.distance_m = Number(kv.dist || 0);
    evt.loopA = kv.a || null;
    evt.loopB = kv.b || null;
    evt.measurementType = kv.type || null;
    return evt;
  }
  if (message.startsWith('POWER|')) {
    const kv = parsePipeKv(message.substring('POWER|'.length));
    evt.type = 'power';
    evt.power = {
      battery: Number(kv.bat || 0),
      solar: Number(kv.sol || 0),
      low: Number(kv.low || 0) === 1
    };
    return evt;
  }
  if (message.startsWith('BATTERY_LOW|') || message.startsWith('BATTERY_OK|')) {
    evt.type = 'battery';
    evt.power = { battery: Number((parsePipeKv(message.substring(message.indexOf('|') + 1)).bat || 0)) };
    return evt;
  }
  if (message.startsWith('TRAFFIC_REPORT')) {
    evt.type = 'traffic-report';
    Object.assign(evt, parseTrafficReport(message, deviceId));
    return evt;
  }
  if (message.startsWith('STATUS|') || message.startsWith('CONFIG|') || message.startsWith('NOISE')) {
    evt.type = 'status';
    const kv = parsePipeKv(message);
    Object.assign(evt, kv);
    return evt;
  }
  evt.type = 'raw';
  return evt;
}

function resolveBoardId(client, topic, message) {
  const candidate = [];

  // 1) Preferred: <id> segment from vehicles/<id>/... topic
  const topicSegments = String(topic || '').split('/').filter(Boolean);
  if (topicSegments.length >= 3 && topicSegments[0] === 'vehicles') {
    const seg = topicSegments[1];
    if (seg && !['+', '#'].includes(seg)) candidate.push(seg);
  }

  if (client && client.id && !isInternalClientId(client.id)) candidate.push(String(client.id));

  const mqttIdMatch = message.match(/(?:^|[|])MQTT_ID\|([^|]+)/i);
  if (mqttIdMatch && mqttIdMatch[1]) candidate.push(String(mqttIdMatch[1]).trim());

  const genericIdMatch = message.match(/(?:^|[|])(device_id|board_id|client_id|id)\:([^|]+)/i);
  if (genericIdMatch && genericIdMatch[2]) candidate.push(String(genericIdMatch[2]).trim());

  if (topicSegments.length) {
    topicSegments.forEach((segment) => {
      if (segment && !['vehicles', 'commands', 'command_responses', 'events', 'report', 'status', 'power', 'speed', '+', '#'].includes(segment)) candidate.push(segment);
    });
  }

  for (const value of candidate) {
    const cleaned = value.trim();
    if (cleaned && cleaned !== 'unknown') return cleaned;
  }

  return 'unknown';
}

function setDeviceStatus(deviceId, status, meta = {}) {
  let device = findDevice(deviceId);
  if (!device) {
    device = {
      id: deviceId,
      name: `Board-${String(deviceId).slice(-4) || 'new'}`,
      status: status,
      lastSeen: new Date().toISOString(),
      power: { battery: 0, solar: 0 },
      network: { signal: 0 },
      loopConfig: {},
      classification: {},
      profile: defaultProfile(),
      stats: {},
      events: [],
      trafficReports: [],
      history: [],
      ...meta
    };
    state.devices.push(device);
  }

  device.status = status;
  device.lastSeen = new Date().toISOString();
  return device;
}

function updateDeviceFromMessage(deviceId, message) {
  let device = setDeviceStatus(deviceId, 'online');

  const evt = parseEventMessageLocal(deviceId, message);
  device.lastSeen = new Date().toISOString();
  device.status = 'online';
  device.everConnected = true;

  if (evt.type === 'identity' && evt.boardId) {
    device.id = evt.boardId;
    if (!device.name || device.name.startsWith('Board-')) device.name = `Board-${String(evt.boardId).slice(-4)}`;
  }

  if (evt.type === 'power' && evt.power) {
    device.power = { ...device.power, ...evt.power };
  }

  if (evt.type === 'status') {
    device.statusInfo = evt;
  }

  if (evt.type === 'traffic-report') {
    device.stats = { ...device.stats, ...evt };
    device.trafficReports = Array.isArray(device.trafficReports) ? device.trafficReports : [];
    device.trafficReports.push({
      ts: evt.ts,
      total: evt.total,
      avg_speed: evt.avg_speed,
      speed_viol: evt.speed_viol,
      dist_viol: evt.dist_viol,
      lane_viol: evt.lane_viol,
      classes: evt.classes || {}
    });
    if (device.trafficReports.length > 200) device.trafficReports = device.trafficReports.slice(-200);
  }

  if (evt.type === 'event') {
    device.events = Array.isArray(device.events) ? device.events : [];
    device.events.push(evt);
    if (device.events.length > 200) device.events = device.events.slice(-200);
  }

  if (evt.type === 'speed') {
    device.events = Array.isArray(device.events) ? device.events : [];
    device.events.push(evt);
    if (device.events.length > 200) device.events = device.events.slice(-200);
    device.stats = device.stats || {};
    device.stats.speedSamples = (device.stats.speedSamples || 0) + 1;
    const totalSpeed = (device.stats.totalSpeed || 0) + (evt.speed_kmh || 0);
    device.stats.totalSpeed = totalSpeed;
    device.stats.lastSpeedKmh = evt.speed_kmh;
    device.stats.avgSpeedKmh = totalSpeed / device.stats.speedSamples;
  }

  // Pair RSP|<cmd_id>|... responses with the originating CMD entry, if any.
  const { cmdId } = unwrapCommandEnvelope(message);
  if (cmdId && state.pendingCommands.has(cmdId)) {
    const pending = state.pendingCommands.get(cmdId);
    state.pendingCommands.delete(cmdId);
    pending.replyTs = new Date().toISOString();
    pending.replyBody = message;
    pending.status = 'replied';
    device.commandLog = Array.isArray(device.commandLog) ? device.commandLog : [];
    device.commandLog.push(pending);
    if (device.commandLog.length > 100) device.commandLog = device.commandLog.slice(-100);
    device.lastReply = pending.replyBody;
  }

  device.history.push({ ts: new Date().toISOString(), message, type: evt.type });
  if (device.history.length > 200) device.history = device.history.slice(-200);

  saveDevices();
  broadcastState();
}

function trackClientLifecycle() {
  broker.on('client', (client) => {
    if (!client || !client.id) return;
    // The dashboard's own MQTT clients (bridge, command publisher) must not
    // be promoted to a "board" device — only real boards should be tracked.
    if (isInternalClientId(client.id)) return;
    state.clients.set(client.id, {
      id: client.id,
      connectedAt: new Date().toISOString()
    });
    const device = findDevice(client.id) || setDeviceStatus(client.id, 'online');
    device.everConnected = true;
    if (!device.name || device.name.startsWith('Board-')) {
      device.name = `Board-${String(client.id).slice(-4)}`;
    }
    saveDevices();
    broadcastState();
  });

  broker.on('clientDisconnect', (client) => {
    if (!client || !client.id) return;
    if (isInternalClientId(client.id)) {
      state.clients.delete(client.id);
      return;
    }
    const known = findDevice(client.id);
    if (known) {
      known.status = 'offline';
      known.lastSeen = new Date().toISOString();
    }
    state.clients.delete(client.id);
    saveDevices();
    broadcastState();
  });
}

function isInternalClientId(id) {
  if (!id) return false;
  return /^dashboard-(bridge|cmd)-/i.test(String(id));
}

function computeGlobalAnalytics() {
  const allReports = state.devices.flatMap((d) => d.trafficReports || []);
  const totalVehicles = allReports.reduce((sum, r) => sum + Number(r.total || 0), 0);
  const avgSpeed = allReports.length
    ? allReports.reduce((sum, r) => sum + Number(r.avg_speed || 0), 0) / allReports.length
    : 0;
  const violations = allReports.reduce((sum, r) => sum + Number(r.speed_viol || 0) + Number(r.dist_viol || 0) + Number(r.lane_viol || 0), 0);

  const classBreakdown = {};
  allReports.forEach((r) => {
    Object.keys(r.classes || {}).forEach((cls) => {
      const entry = r.classes[cls];
      classBreakdown[cls] = (classBreakdown[cls] || 0) + Number(entry.cnt || 0);
    });
  });

  const speedSeries = allReports.map((r) => ({ ts: r.ts, value: Number(r.avg_speed || 0) }));

  return {
    totalBoards: state.devices.length,
    onlineBoards: state.devices.filter((d) => d.status === 'online').length,
    totalVehicles,
    avgSpeed,
    violations,
    classBreakdown,
    speedSeries,
    events: state.devices.flatMap((d) => (d.events || []).slice(-10))
  };
}

function broadcastState() {
  const analytics = computeGlobalAnalytics();
  const payload = JSON.stringify({
    type: 'state',
    devices: state.devices.map(buildDeviceSummary),
    stats: analytics
  });
  wsServer.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}

const brokerServer = require('net').createServer(broker.handle);

broker.on('clientError', (client, err) => {
  console.error('[mqtt-broker] client error:', err.message);
  recordMessage('err', '', err.message, client?.id);
});

broker.on('publish', (packet, client) => {
  if (!packet || !packet.topic) return;
  const topic = packet.topic;
  const payload = packet.payload ? packet.payload.toString() : '';
  const clientId = client?.id || '';
  const isCommandTopic = topic === 'vehicles/commands' || /\/commands$/.test(topic);
  const kind = isCommandTopic ? 'out' : 'in';
  recordMessage(kind, topic, payload, clientId);
  if (
    /\/events$/.test(topic) ||
    /\/command_responses$/.test(topic) ||
    /\/speed$/.test(topic) ||
    topic === 'vehicles/events' ||
    topic === 'vehicles/command_responses' ||
    topic.includes('report') ||
    topic.includes('status') ||
    topic.includes('power')
  ) {
    const deviceId = resolveBoardId(client, topic, payload);
    if (deviceId !== 'unknown') updateDeviceFromMessage(deviceId, normalizePayload(payload));
  }
});

// MQTT client to subscribe to board topics and relay into the dashboard state.
const edgeClient = mqtt.connect('mqtt://localhost:' + mqttPort, { clientId: 'dashboard-bridge-' + uuidv4() });
edgeClient.on('connect', () => {
  const subs = [
    'vehicles/+/events',
    'vehicles/+/command_responses',
    'vehicles/+/commands',
    'vehicles/+/speed',
    'vehicles/events',
    'vehicles/command_responses',
    'vehicles/commands'
  ];
  subs.forEach((t) => edgeClient.subscribe(t));
  console.log('[dashboard] subscribed to per-board and legacy topics');
});
edgeClient.on('message', (topic, payload) => {
  const rawPayload = payload.toString();
  recordMessage(/\/commands$/.test(topic) || topic === 'vehicles/commands' ? 'out' : 'in', topic, rawPayload, 'dashboard-bridge');
  if (rawPayload.startsWith('CMD|')) return;
  // Prefer the <id> segment from the topic; otherwise try payload heuristics.
  const resolved = resolveBoardId(null, topic, rawPayload);
  if (resolved !== 'unknown') updateDeviceFromMessage(resolved, normalizePayload(rawPayload));
});

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/devices', (req, res) => {
  res.json({ devices: state.devices.map(buildDeviceSummary), stats: computeGlobalAnalytics() });
});

app.get('/api/messages', (req, res) => {
  res.json({ messages: state.messages });
});

app.get('/api/pending-commands', (req, res) => {
  res.json({ pending: Array.from(state.pendingCommands.values()) });
});

app.get('/api/device/:id', (req, res) => {
  const d = findDevice(req.params.id);
  res.json(d ? buildDeviceSummary(d) : { error: 'not_found' });
});

app.post('/api/device/:id/name', (req, res) => {
  const d = findDevice(req.params.id);
  if (!d) return res.status(404).json({ error: 'not_found' });
  d.name = String(req.body?.name || d.name);
  d.profile = d.profile || defaultProfile();
  d.profile.name = d.name;
  saveDevices();
  broadcastState();
  res.json({ ok: true, device: buildDeviceSummary(d) });
});

app.post('/api/device/:id/profile', (req, res) => {
  const d = findDevice(req.params.id);
  if (!d) return res.status(404).json({ error: 'not_found' });
  const incoming = req.body || {};
  d.profile = { ...defaultProfile(), ...(d.profile || {}), ...incoming };
  d.name = incoming.name || d.name || d.profile.name || d.id;
  d.profile.name = d.name;
  d.loopConfig = incoming.loopPairs || d.loopConfig || d.profile.loopPairs || [];
  d.classification = incoming.classification || d.classification || d.profile.classification || {};
  saveDevices();
  broadcastState();
  res.json({ ok: true, device: buildDeviceSummary(d) });
});

let commandClient = null;
function getCommandClient() {
  if (!commandClient || !commandClient.connected) {
    commandClient = mqtt.connect('mqtt://localhost:' + mqttPort, { clientId: 'dashboard-cmd-' + uuidv4() });
  }
  return commandClient;
}

app.post('/api/device/:id/command', (req, res) => {
  const d = findDevice(req.params.id);
  if (!d) return res.status(404).json({ error: 'not_found' });
  const cmd = String(req.body?.command || '');
  const broadcast = req.body?.broadcast === true;
  if (!cmd) return res.status(400).json({ error: 'empty_command' });
  const cmdId = String(++state.cmdCounter);
  const envelope = `CMD|${cmdId}|${cmd}`;
  const topics = [`vehicles/${encodeURIComponent(d.id)}/commands`];
  if (broadcast) topics.push('vehicles/commands');
  const client = getCommandClient();
  let pending = false;
  topics.forEach((t) => client.publish(t, envelope, { qos: 0 }, () => {}));
  state.pendingCommands.set(cmdId, {
    cmdId,
    boardId: d.id,
    command: cmd,
    broadcast,
    topics,
    ts: new Date().toISOString(),
    status: 'pending'
  });
  res.json({ ok: true, cmdId, topics, command: cmd });
});

app.delete('/api/device/:id', (req, res) => {
  const deviceId = req.params.id;
  const idx = state.devices.findIndex((d) => d.id === deviceId);
  if (idx === -1) return res.status(404).json({ error: 'not_found' });

  const device = state.devices[idx];
  // Only allow deletion of offline devices
  if (device.status === 'online') {
    return res.status(400).json({ error: 'Cannot delete online device. Wait for it to go offline first.' });
  }

  // Remove from array
  state.devices.splice(idx, 1);
  saveDevices();
  broadcastState();
  res.json({ ok: true, deletedId: deviceId });
});

app.get('/api/analytics', (req, res) => {
  const summary = computeGlobalAnalytics();
  const deviceStats = state.devices.map((d) => ({
    id: d.id,
    name: d.name,
    total: Number(d.stats?.total || 0),
    avg_speed: Number(d.stats?.avg_speed || 0),
    violations: Number(d.stats?.speed_viol || 0) + Number(d.stats?.dist_viol || 0) + Number(d.stats?.lane_viol || 0),
    battery: d.power?.battery ?? null
  }));

  res.json({
    summary,
    deviceStats,
    generatedAt: new Date().toISOString()
  });
});

app.get('/api/health', (req, res) => {
  res.json({ ok: true, mqttPort, httpPort, boards: state.devices.length });
});

// --- OTA Firmware endpoints ---

// List available firmware files
app.get('/api/firmware', (req, res) => {
  try {
    ensureDataDir();
    if (!fs.existsSync(firmwareDir)) return res.json({ files: [] });
    const files = fs.readdirSync(firmwareDir)
      .filter(f => f.endsWith('.bin'))
      .map(f => {
        const stats = fs.statSync(path.join(firmwareDir, f));
        return {
          name: f,
          size: stats.size,
          modified: stats.mtime.toISOString()
        };
      });
    res.json({ files });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Upload firmware file
const upload = multer({ dest: firmwareDir, limits: { fileSize: 3 * 1024 * 1024 } });

app.post('/api/firmware/upload', upload.single('firmware'), (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }
    if (!req.file.originalname.endsWith('.bin')) {
      // Remove non-.bin file
      fs.unlinkSync(req.file.path);
      return res.status(400).json({ error: 'Only .bin files are allowed' });
    }
    // Rename to original name if needed
    const targetPath = path.join(firmwareDir, req.file.originalname);
    if (req.file.path !== targetPath) {
      if (fs.existsSync(targetPath)) fs.unlinkSync(targetPath);
      fs.renameSync(req.file.path, targetPath);
    }
    // Compute MD5
    const fileBuffer = fs.readFileSync(targetPath);
    const md5Hash = crypto.createHash('md5').update(fileBuffer).digest('hex');
    
    res.json({ 
      ok: true, 
      filename: req.file.originalname, 
      size: fileBuffer.length,
      md5: md5Hash,
      url: `/firmware/${req.file.originalname}`
    });
  } catch (err) {
    if (req.file && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path);
    res.status(500).json({ error: err.message });
  }
});

// Serve firmware files (HTTP for board download)
app.get('/firmware/:filename', (req, res) => {
  const filename = req.params.filename;
  if (!filename.endsWith('.bin')) {
    return res.status(400).send('Only .bin files allowed');
  }
  const filepath = path.join(firmwareDir, filename);
  if (!fs.existsSync(filepath)) {
    return res.status(404).send('Firmware not found');
  }
  res.setHeader('Content-Type', 'application/octet-stream');
  res.setHeader('Content-Length', fs.statSync(filepath).size);
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.sendFile(filepath);
});

// Delete firmware file
app.delete('/api/firmware/:filename', (req, res) => {
  try {
    const filename = req.params.filename;
    if (!filename.endsWith('.bin')) {
      return res.status(400).json({ error: 'Only .bin files allowed' });
    }
    const filepath = path.join(firmwareDir, filename);
    if (!fs.existsSync(filepath)) {
      return res.status(404).json({ error: 'Firmware not found' });
    }
    fs.unlinkSync(filepath);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// OTA Update command
app.post('/api/device/:id/ota', (req, res) => {
  const d = findDevice(req.params.id);
  if (!d) return res.status(404).json({ error: 'not_found' });
  
  const { firmwareUrl, md5Hash } = req.body;
  if (!firmwareUrl) return res.status(400).json({ error: 'firmwareUrl required' });
  
  // Build full URL for locally hosted files
  let fullUrl = firmwareUrl;
  if (firmwareUrl.startsWith('/firmware/') || firmwareUrl.startsWith('firmware/')) {
    const filename = firmwareUrl.replace(/^\/firmware\//, '').replace(/^firmware\//, '');
    // Use http (board only speaks HTTP) with the host the browser sees
    const host = req.get('host') || `localhost:${httpPort}`;
    fullUrl = `http://${host}/firmware/${filename}`;
  }
  
  // Build OTA command per protocol
  const cmd = `OTA_UPDATE|${fullUrl}|${md5Hash || ''}`;
  const cmdId = String(++state.cmdCounter);
  const envelope = `CMD|${cmdId}|${cmd}`;
  const topics = [`vehicles/${encodeURIComponent(d.id)}/commands`];
  
  const client = getCommandClient();
  topics.forEach((t) => client.publish(t, envelope, { qos: 0 }, () => {}));
  
  state.pendingCommands.set(cmdId, {
    cmdId,
    boardId: d.id,
    command: cmd,
    broadcast: false,
    topics,
    ts: new Date().toISOString(),
    status: 'pending'
  });
  
  res.json({ ok: true, cmdId, topics, command: cmd, firmwareUrl: fullUrl, md5Hash });
});

wsServer.on('connection', (socket) => {
  socket.send(JSON.stringify({
    type: 'state',
    devices: state.devices.map(buildDeviceSummary),
    stats: computeGlobalAnalytics(),
    messages: state.messages
  }));
});

loadDevices();
trackClientLifecycle();

// Periodically sweep: a device that hasn't been seen in OFFLINE_AFTER_MS is
// considered offline even if its broker connection was missed (e.g. broker
// restart, abrupt network drop). This guarantees the UI reflects reality.
const OFFLINE_AFTER_MS = 60_000;
const PENDING_TIMEOUT_MS = 30_000;
setInterval(() => {
  const now = Date.now();
  let changed = false;

  state.devices.forEach((d) => {
    if (d.status !== 'online') return;
    const last = d.lastSeen ? new Date(d.lastSeen).getTime() : 0;
    if (!last || now - last > OFFLINE_AFTER_MS) {
      d.status = 'offline';
      changed = true;
    }
  });

  // Time out unanswered commands so the UI doesn't keep them as "pending".
  state.pendingCommands.forEach((entry, id) => {
    if (entry.status !== 'pending') return;
    if (now - new Date(entry.ts).getTime() > PENDING_TIMEOUT_MS) {
      entry.status = 'timeout';
      entry.replyTs = new Date().toISOString();
      const device = findDevice(entry.boardId);
      if (device) {
        device.commandLog = Array.isArray(device.commandLog) ? device.commandLog : [];
        device.commandLog.push(entry);
        if (device.commandLog.length > 100) device.commandLog = device.commandLog.slice(-100);
      }
      state.pendingCommands.delete(id);
      changed = true;
    }
  });

  if (changed) {
    saveDevices();
    broadcastState();
  }
}, 10_000);

brokerServer.listen(mqttPort, () => {
  console.log(`[mqtt-broker] listening on port ${mqttPort}`);
});

server.listen(httpPort, () => {
  console.log(`[dashboard] web app listening on http://localhost:${httpPort}`);
});
