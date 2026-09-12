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
