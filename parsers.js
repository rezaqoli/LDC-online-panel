// parsers.js — pure helper functions extracted from server.js so they can
// be unit-tested without spinning up an MQTT broker.

function normalizePayload(raw) {
  const msg = typeof raw === 'string' ? raw : String(raw ?? '');
  return msg.trim();
}

function parsePipeKv(line) {
  const obj = {};
  if (!line) return obj;
  const parts = line.split('|');
  for (const part of parts) {
    if (!part || !part.includes(':')) continue;
    const idx = part.indexOf(':');
    const key = part.slice(0, idx).trim();
    const value = part.slice(idx + 1).trim();
    obj[key] = value;
  }
  return obj;
}

function unwrapCommandEnvelope(raw) {
  if (!raw) return { cmdId: null, body: '' };
  if (raw.startsWith('CMD|')) {
    const rest = raw.substring(4);
    const sep = rest.indexOf('|');
    if (sep < 0) return { cmdId: rest.trim(), body: '' };
    return { cmdId: rest.substring(0, sep).trim(), body: rest.substring(sep + 1) };
  }
  if (raw.startsWith('RSP|')) {
    const rest = raw.substring(4);
    const sep = rest.indexOf('|');
    if (sep < 0) return { cmdId: rest.trim(), body: '' };
    return { cmdId: rest.substring(0, sep).trim(), body: rest.substring(sep + 1) };
  }
  return { cmdId: null, body: raw };
}

// Internal client ids that the dashboard itself uses (bridge/cmd). These must
// not be promoted to board devices — only real ESP32/board client ids count.
function isInternalClientId(id) {
  if (!id) return false;
  return /^dashboard-(bridge|cmd)-/i.test(String(id));
}

// Resolve a board id from a publish context. The MQTT_ID|<value> token in the
// payload is the canonical board identity and wins over topic segments and
// transient client ids. The function is exported so the detection logic can be
// unit-tested without spinning up an MQTT broker.
function resolveBoardId(client, topic, message) {
  const candidate = [];

  // 1) Canonical identity: MQTT_ID|<value> at start of body or after a pipe.
  //    Tolerate whitespace around the value.
  const mqttIdMatch = String(message || '').match(/(?:^|[|]\s*)MQTT_ID\s*[|:]\s*([^|,\r\n]+)/i);
  if (mqttIdMatch && mqttIdMatch[1]) {
    candidate.push(String(mqttIdMatch[1]).trim());
  }

  // 2) Preferred: <id> segment from vehicles/<id>/... topic
  const topicSegments = String(topic || '').split('/').filter(Boolean);
  if (topicSegments.length >= 3 && topicSegments[0] === 'vehicles') {
    const seg = topicSegments[1];
    if (seg && !['+', '#'].includes(seg)) candidate.push(seg);
  }

  // 3) Non-internal client id from the broker connection event.
  if (client && client.id && !isInternalClientId(client.id)) {
    candidate.push(String(client.id));
  }

  // 4) Generic key:value identifiers (device_id, board_id, id, client_id).
  const genericIdMatch = String(message || '').match(/(?:^|[|])(device_id|board_id|client_id|id)\s*:\s*([^|,\r\n]+)/i);
  if (genericIdMatch && genericIdMatch[2]) {
    candidate.push(String(genericIdMatch[2]).trim());
  }

  // 5) Any other topic segment that looks like a board id.
  if (topicSegments.length) {
    topicSegments.forEach((segment) => {
      if (segment && !['vehicles', 'commands', 'command_responses', 'events', 'report', 'status', 'power', 'speed', '+', '#'].includes(segment)) {
        candidate.push(segment);
      }
    });
  }

  for (const value of candidate) {
    const cleaned = String(value || '').trim();
    if (cleaned && cleaned !== 'unknown') return cleaned;
  }
  return 'unknown';
}

function parseTrafficReport(line, deviceId) {
  const obj = parsePipeKv(line);
  const out = {
    deviceId,
    ts: obj.ts || new Date().toISOString(),
    duration_s: Number(obj.dur_s || 0),
    total: Number(obj.total || 0),
    avg_speed: Number(obj.avg_speed || 0),
    speed_viol: Number(obj.speed_viol || 0),
    dist_viol: Number(obj.dist_viol || 0),
    lane_viol: Number(obj.lane_viol || 0),
    classes: {}
  };
  Object.keys(obj).forEach((key) => {
    const m = key.match(/^([A-Za-z0-9_+]+)_(cnt|avg)$/);
    if (m) {
      const cls = m[1];
      const field = m[2];
      if (!out.classes[cls]) out.classes[cls] = {};
      out.classes[cls][field] = Number(obj[key]);
    }
  });
  return out;
}

function parseEventMessage(deviceId, message) {
  const evt = { deviceId, raw: message, ts: new Date().toISOString() };
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
    evt.length_m  = Number(kv.len || 0);
    evt.delay_ms  = Number(kv.delay || 0);
    evt.distance_m = Number(kv.dist || 0);
    evt.loopA = kv.a || null;
    evt.loopB = kv.b || null;
    evt.measurementType = kv.type || null;
    return evt;
  }
  return evt;
}

module.exports = {
  normalizePayload,
  parsePipeKv,
  unwrapCommandEnvelope,
  parseTrafficReport,
  parseEventMessage,
  isInternalClientId,
  resolveBoardId,
};
