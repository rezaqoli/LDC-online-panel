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
};
