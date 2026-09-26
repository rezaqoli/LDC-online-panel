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

function numberValue(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : undefined;
}

// Merge configuration/status replies emitted by processSystemCommand into a
// dashboard profile. Unknown replies are intentionally ignored.
function applyConfigurationReply(profile = {}, raw = '') {
  const { body } = unwrapCommandEnvelope(normalizePayload(raw));
  const next = JSON.parse(JSON.stringify(profile || {}));
  const ensure = (key) => (next[key] ||= {});
  const n = (v) => numberValue(v);

  if (body.startsWith('CONFIG|')) {
    const kv = parsePipeKv(body);
    const detector = ensure('detector');
    const classification = ensure('classification');
    const detectorMap = {
      enter: 'enter_thresh', abs: 'absolute_min_dev', exit_ratio: 'exit_ratio', hyst: 'exit_hysteresis_cnt',
      min_ms: 'min_event_ms', max_ms: 'max_event_ms', prom: 'peak_prominence_ratio', axle_ms: 'min_axle_distance_ms',
      confirm: 'confirm_samples', min_samples: 'min_event_samples', peak_ratio: 'peak_to_baseline_ratio', enter_hyst: 'enter_hysteresis_ratio',
      exit_hyst: 'exit_hysteresis_ratio', enter_sigma: 'enter_sigma', abs_sigma: 'abs_sigma', default_kmh: 'default_speed_kmh',
      deriv_sigma: 'derivative_sigma', deriv_slow_sigma: 'derivative_slow_sigma', deriv_window_ms: 'derivative_slow_window_ms'
    };
    const classMap = {
      motor: 'motor_max_len', car: 'car_max_len', pickup: 'pickup_max_len', van: 'van_max_len', bus: 'bus_max_len',
      truckS: 'truck_s_max_len', truck2: 'truck_2_max_len', truck3: 'truck_3_max_len', truck4min: 'truck_4_plus_min_len',
      rise_short: 'rise_short_ms', rise_mid: 'rise_mid_ms', rise_long: 'rise_long_ms', energy_low: 'energy_low',
      energy_mid: 'energy_mid', energy_high: 'energy_high', crest_spiky: 'crest_spiky', crest_broad: 'crest_broad',
      skew_tol: 'skew_tol', skew_high: 'skew_high', com_min: 'com_min', com_max: 'com_max', width_mid: 'width_mid',
      width_wide: 'width_wide', std_high: 'std_high', std_low: 'std_low'
    };
    Object.entries(detectorMap).forEach(([wire, key]) => { if (n(kv[wire]) !== undefined) detector[key] = n(kv[wire]); });
    Object.entries(classMap).forEach(([wire, key]) => { if (n(kv[wire]) !== undefined) classification[key] = n(kv[wire]); });
    if (kv.auto !== undefined) detector.auto_threshold = Number(kv.auto) !== 0;
    if (kv.entry_mode !== undefined) detector.entry_mode = kv.entry_mode;
    // GET_CONFIG always includes pair 0 today, but only merge fields that were
    // actually present so partial/older firmware replies cannot erase a profile.
    if (['dual', 'dist', 's1', 'c1', 's2', 'c2'].some((key) => kv[key] !== undefined)) {
      next.loopPairs ||= [];
      const pair = { ...(next.loopPairs[0] || {}) };
      if (kv.dual !== undefined) pair.enabled = Number(kv.dual) !== 0;
      const pairMap = { dist: 'distance_m', s1: 'sensor1', c1: 'ch1', s2: 'sensor2', c2: 'ch2' };
      Object.entries(pairMap).forEach(([wire, key]) => { if (n(kv[wire]) !== undefined) pair[key] = n(kv[wire]); });
      next.loopPairs[0] = pair;
    }
  } else if (body.startsWith('CONFIG_ACK|')) {
    const kv = parsePipeKv(body);
    const positional = body.split('|').slice(1).filter((x) => !x.includes(':'));
    const idx = n(kv.idx ?? positional[0]);
    if (idx !== undefined) {
      next.loopPairs ||= [];
      next.loopPairs[idx] = { ...(next.loopPairs[idx] || {}), enabled: Number(kv.dual ?? positional[1]) !== 0,
        distance_m: n(kv.dist ?? positional[2]), sensor1: n(kv.s1 ?? positional[3]), ch1: n(kv.c1 ?? positional[4]),
        sensor2: n(kv.s2 ?? positional[5]), ch2: n(kv.c2 ?? positional[6]) };
    }
  } else if (body.startsWith('RULES_ACK|')) {
    const kv = parsePipeKv(body); const rules = ensure('rules');
    const map = { limit: 'speed_limit_kmh', tol: 'speed_tolerance_kmh', min_dist: 'min_follow_distance_m', min_headway: 'min_headway_s', max_headway: 'max_headway_ms', straddle_ms: 'min_straddle_overlap_ms', straddle_ratio: 'min_straddle_overlap_ratio', assume_kmh: 'assume_speed_kmh' };
    Object.entries(map).forEach(([wire, key]) => { if (n(kv[wire]) !== undefined) rules[key] = n(kv[wire]); });
  } else if (body.startsWith('REPORT_CFG|')) {
    const p = body.split('|'); next.report = { ...(next.report || {}), enabled: Number(p[1]) !== 0, interval_min: (n(p[2]) || 0) / 60000, clear_on_report: Number(p[3]) !== 0 };
  } else if (body.startsWith('MQTT_CFG|')) {
    const p = body.split('|'); next.mqtt = { ...(next.mqtt || {}), id: p[1] || '', server: p[2] || '', port: n(p[3]) || 0, ip: p[4] || '', user: p[5] || '', pass: p[6] || '', apn: p[7] || '', topic_events: p[8] || '', topic_commands: p[9] || '', topic_responses: p[10] || '' };
  } else if (body.startsWith('SENSOR_LC|')) {
    const p = body.split('|').slice(1); next.sensorLC ||= [];
    for (let i = 0; i + 3 < p.length; i += 4) { const m = p[i].match(/^s(\d)c(\d):(.+)$/); if (m) next.sensorLC[Number(m[1]) * 4 + Number(m[2])] = { sensor: Number(m[1]), channel: Number(m[2]), l: n(m[3]), c: n(p[i + 1]), conversion_time: n(p[i + 2]), driver_current: n(p[i + 3]) }; }
  } else if (body.startsWith('DEFAULT_KMH|')) {
    ensure('detector').default_speed_kmh = n(body.split('|')[1]);
  }
  return next;
}

module.exports = {
  normalizePayload,
  parsePipeKv,
  unwrapCommandEnvelope,
  parseTrafficReport,
  parseEventMessage,
  applyConfigurationReply,
};
