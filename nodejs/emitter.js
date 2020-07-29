const logger = require('fluent-logger');
logger.configure('data', {
    host: 'localhost',
    port: 24224,
    timeout: 3.0,
    reconnectInterval: 5000
});

const testData = [
    {a: "a1",  b: "1595992901", c: null, d: null, e: null, f: null, g: null, h: null,   i: null, j: null, k: null},
    {a: "a2",  b: "1595992902", c: 2.5,  d: null, e: null, f: null, g: null, h: null,   i: null, j: null, k: null},
    {a: "a3",  b: "1595992903", c: null, d: 1,    e: null, f: null, g: null, h: null,   i: null, j: null, k: null},
    {a: "a4",  b: "1595992904", c: null, d: null, e: null, f: null, g: null, h: null,   i: null, j: null, k: null},
    {a: "a5",  b: "1595992905", c: null, d: null, e: null, f: null, g: null, h: null,   i: null, j: null, k: null},
    {a: "a6",  b: "1595992906", c: null, d: null, e: 2,    f: null, g: null, h: null,   i: null, j: null, k: null},
    {a: "a7",  b: "1595992907", c: null, d: null, e: null, f: "US", g: null, h: null,   i: null, j: null, k: null},
    {a: "a8",  b: "1595992908", c: null, d: null, e: null, f: null, g: null, h: null,   i: null, j: null, k: null},
    {a: "a9",  b: "1595992909", c: null, d: null, e: null, f: null, g: null, h: 3,      i: null, j: null, k: null},
    {a: "a10", b: "1595992910", c: null, d: null, e: null, f: null, g: null, h: "sddd", i: null, j: null, k: null},
    {a: "a11", b: "1595992911", c: null, d: null, e: null, f: null, g: null, h: null,   i: null, j: null, k: null},
    {a: "a12", b: "1595992912", c: null, d: null, e: null, f: null, g: null, h: null,   i: 4.6,  j: null, k: null},
    {a: "a13", b: "1595992913", c: null, d: null, e: null, f: null, g: null, h: null,   i: "af", j: null, k: null},
    {a: "a14", b: "1595992914", c: null, d: null, e: null, f: null, g: null, h: null,   i: null, j: null, k: null},
    {a: "a15", b: "1595992915", c: null, d: null, e: null, f: null, g: null, h: null,   i: null, j: 7.8,  k: null},
    {a: "a16", b: "1595992916", c: null, d: null, e: null, f: null, g: null, h: null,   i: null, j: "wp", k: null},
    {a: "a17", b: "1595992917", c: null, d: null, e: null, f: null, g: null, h: null,   i: null, j: null, k: null},
    {a: "a18", b: "1595992918", c: null, d: null, e: null, f: null, g: null, h: null,   i: null, j: null, k: 3456},
    {a: "a19", b: "1595992919", c: null, d: null, e: null, f: null, g: null, h: null,   i: null, j: null, k: "bidppp"}
];


testData.forEach(e => {
    logger.emit('mylabel', e);
});
