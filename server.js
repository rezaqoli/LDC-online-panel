const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
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
  isInternalClientId,
  resolveBoardId,
} = require('./parsers.js');

const app = express();
const server = http.createServer(app);
const broker = aedes();
const wsServer = new WebSocket.Server({ server, path: '/ws' });
const mqttPort = process.env.MQTT_PORT || 1010;
const httpPort = process.env.PORT || 3000;
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
      { enabled: false, distance_m: 0.4, sensor1: 0, ch1: 0, sensor2: 0, ch2: 1 },
      { enabled: false, distance_m: 0.4, sensor1: 0, ch1: 2, sensor2: 0, ch2: 3 },
      { enabled: false, distance_m: 0.4, sensor1: 1, ch1: 0, sensor2: 1, ch2: 1 },
      { enabled: false, distance_m: 0.4, sensor1: 1, ch1: 2, sensor2: 1, ch2: 3 }
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
      interval_min: 5,
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

function findDeviceByClientId(clientId) {
  const id = String(clientId || '');
  return state.devices.find((d) => d.id === id || (Array.isArray(d.clientIds) && d.clientIds.includes(id)));
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
    ota: device.ota ?? null,
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
  // Responses are commonly wrapped as RSP|<request-id>|<payload>. Parse the
  // payload for its meaning while keeping the complete message in raw.
  const body = unwrapCommandEnvelope(message).body.trim();
  if (body.startsWith('MQTT_ID|')) {
    const value = body.substring('MQTT_ID|'.length).trim();
    evt.type = 'identity';
    evt.boardId = value;
    evt.clientId = value;
    return evt;
  }
  if (body.startsWith('EVENT|')) {
    const kv = parsePipeKv(body.substring('EVENT|'.length));
    Object.assign(evt, kv);
    evt.type = 'event';
    return evt;
  }
  if (body.startsWith('SPEED|')) {
    const kv = parsePipeKv(body.substring('SPEED|'.length));
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
  if (body.startsWith('POWER|')) {
    const kv = parsePipeKv(body.substring('POWER|'.length));
    evt.type = 'power';
    evt.power = {
      battery: Number(kv.bat || 0),
      solar: Number(kv.sol || 0),
      low: Number(kv.low || 0) === 1
    };
    return evt;
  }
  if (body.startsWith('BATTERY_LOW|') || body.startsWith('BATTERY_OK|')) {
    evt.type = 'battery';
    evt.power = { battery: Number((parsePipeKv(body.substring(body.indexOf('|') + 1)).bat || 0)) };
    return evt;
  }
  if (body.startsWith('TRAFFIC_REPORT')) {
    evt.type = 'traffic-report';
    Object.assign(evt, parseTrafficReport(body, deviceId));
    return evt;
  }
  if (body.startsWith('OTA_STATUS|') || body.startsWith('OTA_SUCCESS|') || body.startsWith('OTA_ERROR|')) {
    const separator = body.indexOf('|');
    evt.type = 'ota';
    evt.state = body.substring(0, separator).substring(4).replace(/^_/, '').toLowerCase();
    evt.detail = separator >= 0 ? body.substring(separator + 1) : '';
    return evt;
  }
  if (body.startsWith('OTA_PROGRESS|')) {
    const parts = body.split('|');
    evt.type = 'ota';
    evt.state = 'progress';
    evt.received = Number(parts[1] || 0);
    evt.total = Number(parts[2] || 0);
    evt.percent = evt.total > 0 ? Math.min(100, Math.round((evt.received / evt.total) * 100)) : 0;
    return evt;
  }
  if (body.startsWith('STATUS|') || body.startsWith('CONFIG|') || body.startsWith('NOISE')) {
    evt.type = 'status';
    const kv = parsePipeKv(body);
    Object.assign(evt, kv);
    return evt;
  }
  evt.type = 'raw';
  return evt;
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

function updateDeviceFromMessage(deviceId, message, clientId = '') {
  let device = setDeviceStatus(deviceId, 'online');
  if (clientId && !isInternalClientId(clientId)) {
    device.clientIds = Array.isArray(device.clientIds) ? device.clientIds : [];
    if (!device.clientIds.includes(clientId)) device.clientIds.push(clientId);
  }

  const evt = parseEventMessageLocal(deviceId, message);
  device.lastSeen = new Date().toISOString();
  device.status = 'online';
  device.everConnected = true;

  if (evt.type === 'identity' && evt.boardId) {
    if (device.id !== evt.boardId) {
      const canonical = findDevice(evt.boardId);
      if (canonical && canonical !== device) {
        canonical.clientIds = [...new Set([...(canonical.clientIds || []), ...(device.clientIds || [])])];
        canonical.status = 'online';
        canonical.lastSeen = new Date().toISOString();
        canonical.everConnected = true;
        state.devices = state.devices.filter((entry) => entry !== device);
        device = canonical;
      } else {
        device.id = evt.boardId;
      }
    }
    if (!device.name || device.name.startsWith('Board-')) device.name = `Board-${String(evt.boardId).slice(-4)}`;
  }

  if (evt.type === 'power' && evt.power) {
    device.power = { ...device.power, ...evt.power };
  }

  if (evt.type === 'status') {
    device.statusInfo = evt;
  }

  if (evt.type === 'ota') {
    device.ota = {
      state: evt.state,
      received: evt.received ?? null,
      total: evt.total ?? null,
      percent: evt.percent ?? null,
      ts: evt.ts,
      raw: evt.raw
    };
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
    const device = findDeviceByClientId(client.id) || setDeviceStatus(client.id, 'online');
    device.clientIds = Array.isArray(device.clientIds) ? device.clientIds : [];
    if (!device.clientIds.includes(client.id)) device.clientIds.push(client.id);
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
    const known = findDeviceByClientId(client.id);
    if (known) {
      known.clientIds = (known.clientIds || []).filter((id) => id !== client.id);
      known.status = known.clientIds.length ? 'online' : 'offline';
      known.lastSeen = new Date().toISOString();
    }
    state.clients.delete(client.id);
    saveDevices();
    broadcastState();
  });
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
  const topic = String(packet.topic);
  const payload = packet.payload ? packet.payload.toString() : '';
  const clientId = client?.id || '';
  const isCommandTopic = topic === 'vehicles/commands' || /\/commands$/.test(topic);
  const kind = isCommandTopic ? 'out' : 'in';
  if (process.env.DEBUG_MQTT === '1') {
    console.log(`[mqtt] ${kind} ${topic} (client=${clientId || '-'}) ${payload}`);
  }
});

// MQTT client to subscribe to board topics and relay into the dashboard state.
// By default it connects to the local aedes broker, but when the ESP32
// publishes to a different broker (e.g. a cellular MQTT relay at 1011),
// set MQTT_URL=mqtt://host:port to point the bridge at the real broker.
const edgeBrokerUrl = process.env.MQTT_URL || ('mqtt://localhost:' + mqttPort);
const mqttClientOptions = { protocolVersion: 4, clean: true };
const edgeClient = mqtt.connect(edgeBrokerUrl, {
  ...mqttClientOptions,
  clientId: 'dashboard-bridge-' + uuidv4()
});
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
  console.log(`[dashboard] bridge connected to ${edgeBrokerUrl}, subscribed to per-board and legacy topics`);
});
edgeClient.on('error', (err) => {
  console.error(`[dashboard] bridge error on ${edgeBrokerUrl}: ${err.message}`);
});
edgeClient.on('message', (topic, payload) => {
  const rawPayload = normalizePayload(payload.toString());
  const isCommandTopic = topic === 'vehicles/commands' || /\/commands$/.test(topic);
  recordMessage(isCommandTopic ? 'out' : 'in', topic, rawPayload, 'dashboard-bridge');

  if (!isCommandTopic && (
    /\/events$/.test(topic) ||
    /\/command_responses$/.test(topic) ||
    /\/speed$/.test(topic) ||
    topic === 'vehicles/events' ||
    topic === 'vehicles/command_responses' ||
    topic.includes('report') || topic.includes('status') || topic.includes('power')
  )) {
    let deviceId = resolveBoardId(null, topic, rawPayload);
    if (deviceId === 'unknown' && /(^|\/)command_responses$/.test(topic)) {
      const { cmdId } = unwrapCommandEnvelope(rawPayload);
      const pending = cmdId ? state.pendingCommands.get(cmdId) : null;
      if (pending) deviceId = pending.boardId;
    }
    if (deviceId !== 'unknown') updateDeviceFromMessage(deviceId, rawPayload, 'dashboard-bridge');
  }
});
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// The board pulls this artifact over cellular during OTA. Set PUBLIC_BASE_URL
// when the dashboard is behind a proxy or has a public hostname different from
// the incoming request host.
app.post('/api/firmware', express.raw({ type: ['application/octet-stream', 'application/x-binary'], limit: '3mb' }), (req, res) => {
  if (!Buffer.isBuffer(req.body) || req.body.length < 1024) {
    return res.status(400).json({ error: 'firmware_body_missing_or_too_small' });
  }
  const originalName = String(req.get('x-filename') || 'firmware.bin');
  if (!/\.bin$/i.test(originalName)) return res.status(400).json({ error: 'firmware_must_be_bin' });
  const fileName = `${uuidv4()}.bin`;
  const filePath = path.join(firmwareDir, fileName);
  try {
    fs.writeFileSync(filePath, req.body, { flag: 'wx' });
    const md5 = crypto.createHash('md5').update(req.body).digest('hex');
    const baseUrl = String(process.env.PUBLIC_BASE_URL || `${req.protocol}://${req.get('host')}`).replace(/\/$/, '');
    res.status(201).json({ ok: true, fileName: originalName, bytes: req.body.length, md5, url: `${baseUrl}/firmware/${fileName}` });
  } catch (err) {
    try { fs.unlinkSync(filePath); } catch (_) {}
    res.status(500).json({ error: 'firmware_store_failed' });
  }
});

app.get('/firmware/:file', (req, res) => {
  const file = String(req.params.file || '');
  if (!/^[0-9a-f-]+\.bin$/i.test(file)) return res.sendStatus(404);
  res.sendFile(path.join(firmwareDir, file), (err) => {
    if (err && !res.headersSent) res.sendStatus(err.statusCode === 404 ? 404 : 500);
  });
});

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

app.delete('/api/device/:id', (req, res) => {
  const deviceId = String(req.params.id);
  const device = findDevice(deviceId);
  if (!device) return res.status(404).json({ error: 'not_found' });
  const hasConnectedAlias = [deviceId, ...(device.clientIds || [])].some((id) => state.clients.has(id));
  if (device.status === 'online' || hasConnectedAlias) {
    return res.status(409).json({ error: 'board_is_connected', message: 'Disconnect the board before deleting it.' });
  }

  state.devices = state.devices.filter((d) => d.id !== deviceId);
  state.pendingCommands.forEach((entry, commandId) => {
    if (entry.boardId === deviceId) state.pendingCommands.delete(commandId);
  });
  saveDevices();
  broadcastState();
  res.json({ ok: true, deleted: deviceId });
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
    commandClient = mqtt.connect(edgeBrokerUrl, {
      ...mqttClientOptions,
      clientId: 'dashboard-cmd-' + uuidv4()
    });
  }
  return commandClient;
}

app.post('/api/device/:id/command', (req, res) => {
  const d = findDevice(req.params.id);
  if (!d) return res.status(404).json({ error: 'not_found' });
  const cmd = String(req.body?.command || '');
  const broadcast = req.body?.broadcast === true;
  if (!cmd) return res.status(400).json({ error: 'empty_command' });
  const cmdId = uuidv4();
  const envelope = `CMD|${cmdId}|${cmd}`;
  // A broadcast is global-only. Publishing both would execute the command twice
  // on the selected vehicle, which subscribes to both topic forms.
  const topics = [broadcast ? 'vehicles/commands' : `vehicles/${encodeURIComponent(d.id)}/commands`];
  const client = getCommandClient();
  // Register the correlation entry before publishing. A local/low-latency
  // broker can deliver the board response immediately.
  state.pendingCommands.set(cmdId, {
    cmdId,
    boardId: d.id,
    command: cmd,
    broadcast,
    topics,
    ts: new Date().toISOString(),
    status: 'pending'
  });
  topics.forEach((t) => client.publish(t, envelope, { qos: 0 }, () => {}));
  res.json({ ok: true, cmdId, topics, command: cmd });
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

// Keep API failures machine-readable. Without this, Express returns an HTML
// error page and the browser reports a misleading JSON parse error.
app.use('/api', (req, res) => {
  res.status(404).json({ error: 'api_route_not_found', path: req.path });
});

app.use((err, req, res, next) => {
  if (res.headersSent) return next(err);
  const status = err.status === 413 || err.type === 'entity.too.large' ? 413 : (err.status || 500);
  res.status(status).json({ error: status === 413 ? 'firmware_too_large_max_3mb' : 'server_error' });
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
