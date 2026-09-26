// test/parsers.test.js — Node test runner (node:test) for the dashboard
// parser helpers.

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  normalizePayload,
  parsePipeKv,
  unwrapCommandEnvelope,
  parseTrafficReport,
  parseEventMessage,
  applyConfigurationReply,
} = require('../parsers.js');

test('normalizePayload handles Buffer / object / string', () => {
  assert.equal(normalizePayload('  hi  '), 'hi');
  assert.equal(normalizePayload(Buffer.from('hello')), 'hello');
  assert.equal(normalizePayload({}), '[object Object]');
  assert.equal(normalizePayload(null), '');
  assert.equal(normalizePayload(undefined), '');
});

test('parsePipeKv splits and trims', () => {
  assert.deepEqual(parsePipeKv('a:1|b:2'), { a: '1', b: '2' });
  assert.deepEqual(parsePipeKv('  a : 1 | b:2 '), { a: '1', b: '2' });
  assert.deepEqual(parsePipeKv('a:1|novalue|'), { a: '1' });
  assert.deepEqual(parsePipeKv(''), {});
  assert.deepEqual(parsePipeKv(null), {});
});

test('unwrapCommandEnvelope parses CMD and RSP envelopes', () => {
  assert.deepEqual(unwrapCommandEnvelope('CMD|abc123|SET_LIGHT|on'),
                   { cmdId: 'abc123', body: 'SET_LIGHT|on' });
  assert.deepEqual(unwrapCommandEnvelope('RSP|xyz|RES|ok'),
                   { cmdId: 'xyz', body: 'RES|ok' });
  // Legacy/unaddressed message
  assert.deepEqual(unwrapCommandEnvelope('SET_LIGHT|on'),
                   { cmdId: null, body: 'SET_LIGHT|on' });
  assert.deepEqual(unwrapCommandEnvelope(''), { cmdId: null, body: '' });
});

test('parseTrafficReport counts vehicles and classes', () => {
  const r = parseTrafficReport(
    'TRAFFIC_REPORT|dur_s:1200|total:7|avg_speed:54.2|speed_viol:1|dist_viol:0|lane_viol:0|Car_cnt:4|Car_avg:55|Truck3_cnt:2|Truck3_avg:48',
    'BOARD-01'
  );
  assert.equal(r.deviceId, 'BOARD-01');
  assert.equal(r.duration_s, 1200);
  assert.equal(r.total, 7);
  assert.equal(r.avg_speed, 54.2);
  assert.equal(r.speed_viol, 1);
  assert.equal(r.dist_viol, 0);
  assert.equal(r.lane_viol, 0);
  assert.equal(r.classes.Car.cnt, 4);
  assert.equal(r.classes.Car.avg, 55);
  assert.equal(r.classes.Truck3.cnt, 2);
  assert.equal(r.classes.Truck3.avg, 48);
});

test('parseTrafficReport tolerates missing fields', () => {
  const r = parseTrafficReport('TRAFFIC_REPORT', 'X');
  assert.equal(r.total, 0);
  assert.equal(r.avg_speed, 0);
  assert.deepEqual(r.classes, {});
});

test('parseEventMessage identifies EVENT and SPEED and MQTT_ID', () => {
  const e = parseEventMessage('B1', 'EVENT|S1C0|dur:120|peak:0.05|class:Car');
  assert.equal(e.type, 'event');
  assert.equal(e.dur, '120');
  assert.equal(e.peak, '0.05');
  assert.equal(e.class, 'Car');

  const s = parseEventMessage('B1', 'SPEED|idx:2|speed:78.4|len:4.8|type:Truck3|a:S1C0|b:S2C0');
  assert.equal(s.type, 'speed');
  assert.equal(s.loopIndex, 2);

  const id = parseEventMessage('B1', 'MQTT_ID|ESP32_GEO_07');
  assert.equal(id.type, 'identity');
  assert.equal(id.boardId, 'ESP32_GEO_07');
});

test('parseEventMessage returns raw event for unknown prefix', () => {
  const e = parseEventMessage('B1', 'GNSS|valid:1|lat:35.7|lon:51.4');
  assert.equal(e.type, undefined);
  assert.equal(e.raw, 'GNSS|valid:1|lat:35.7|lon:51.4');
});

test('configuration replies merge into a dashboard profile', () => {
  let profile = applyConfigurationReply({}, 'RSP|c1|CONFIG|enter:1.25|exit_ratio:0.60|hyst:4|min_ms:30|max_ms:4000|auto:1|default_kmh:45|motor:1.3|rise_short:20|dual:1|dist:2.5|s1:0|c1:1|s2:1|c2:2');
  assert.equal(profile.detector.enter_thresh, 1.25);
  assert.equal(profile.detector.exit_hysteresis_cnt, 4);
  assert.equal(profile.detector.auto_threshold, true);
  assert.equal(profile.detector.default_speed_kmh, 45);
  assert.equal(profile.classification.rise_short_ms, 20);
  assert.deepEqual(profile.loopPairs[0], { enabled: true, distance_m: 2.5, sensor1: 0, ch1: 1, sensor2: 1, ch2: 2 });

  profile = applyConfigurationReply(profile, 'RULES_ACK|limit:60|tol:5|min_dist:12|min_headway:1.50|max_headway:5000|straddle_ms:80|straddle_ratio:0.25|assume_kmh:48');
  assert.equal(profile.rules.min_follow_distance_m, 12);
  assert.equal(profile.rules.assume_speed_kmh, 48);

  profile = applyConfigurationReply(profile, 'SENSOR_LC|s0c0:100.500|2.250|120|15|s1c3:99.000|3.000|130|16');
  assert.equal(profile.sensorLC[0].l, 100.5);
  assert.equal(profile.sensorLC[7].driver_current, 16);

  profile = applyConfigurationReply(profile, 'CONFIG|confirm:5|min_samples:8|peak_ratio:1.9|enter_hyst:0.8|exit_hyst:0.6|entry_mode:derivative');
  assert.equal(profile.detector.min_event_samples, 8);
  assert.equal(profile.detector.peak_to_baseline_ratio, 1.9);
  assert.equal(profile.detector.enter_hysteresis_ratio, 0.8);
  assert.equal(profile.detector.entry_mode, 'derivative');
  // A partial CONFIG reply must not erase the existing loop pair.
  assert.equal(profile.loopPairs[0].distance_m, 2.5);
});
