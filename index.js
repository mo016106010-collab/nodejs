'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT          = parseInt(process.env.PORT          || '8767', 10);
const MAX_ROOMS     = parseInt(process.env.MAX_ROOMS     || '500',  10);
const MAX_PER_ROOM  = parseInt(process.env.MAX_PER_ROOM  || '16',   10);
const PING_INTERVAL = parseInt(process.env.PING_INTERVAL || '25000',10);

const rooms = new Map();
let totalClients = 0;

function getOrCreateRoom(ch) { if (!rooms.has(ch)) rooms.set(ch, new Set()); return rooms.get(ch); }
function leaveRoom(c) { if (!c.channel) return; const r = rooms.get(c.channel); if (r) { r.delete(c); if (!r.size) rooms.delete(c.channel); } c.channel = null; }
function joinRoom(c, ch) {
    leaveRoom(c);
    if (ch !== 'rockets:discovery' && rooms.size >= MAX_ROOMS && !rooms.has(ch)) { send(c.ws,{type:'ERROR',code:'ROOM_LIMIT',message:'Server at capacity'}); return false; }
    const r = getOrCreateRoom(ch);
    if (r.size >= MAX_PER_ROOM) { send(c.ws,{type:'ERROR',code:'ROOM_FULL',message:'Room is full'}); return false; }
    r.add(c); c.channel = ch; return true;
}
function broadcast(fc, data) { if (!fc.channel) return; const r = rooms.get(fc.channel); if (!r) return; for (const p of r) { if (p !== fc && p.ws.readyState === WebSocket.OPEN) p.ws.send(data,{binary:false}); } }
function send(ws, obj) { if (ws.readyState === WebSocket.OPEN) try { ws.send(JSON.stringify(obj)); } catch(_){} }

const httpServer = http.createServer((req, res) => {
    res.writeHead(200,{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({status:'ok',service:'Rockets! Relay',clients:totalClients,rooms:rooms.size,uptime:Math.floor(process.uptime())}));
});

const wss = new WebSocketServer({ server: httpServer, path: '/' });
wss.on('connection', (ws) => {
    totalClients++;
    const c = { ws, channel: null, alive: true };
    joinRoom(c, 'rockets:discovery');
    ws.on('message', (raw) => {
        let msg; try { msg = JSON.parse(raw.toString()); } catch(_){ return; }
        if (msg.type === 'JOIN')  { const ch = String(msg.channel||'').trim(); if (ch) { joinRoom(c,ch); send(ws,{type:'JOIN_ACK',channel:ch}); } return; }
        if (msg.type === 'LEAVE') { joinRoom(c,'rockets:discovery'); return; }
        if (msg.type === 'PONG')  { c.alive = true; return; }
        const tgt = (msg.channel && String(msg.channel).trim()) || c.channel;
        if (!tgt) return;
        const orig = c.channel; c.channel = tgt;
        broadcast(c, raw.toString());
        c.channel = orig;
    });
    ws.on('close', () => { totalClients = Math.max(0,totalClients-1); leaveRoom(c); });
    ws.on('error', () => { totalClients = Math.max(0,totalClients-1); leaveRoom(c); });
});

setInterval(() => {
    for (const room of rooms.values())
        for (const c of room) { if (!c.alive) { c.ws.terminate(); continue; } c.alive=false; send(c.ws,{type:'PING'}); }
}, PING_INTERVAL);

httpServer.listen(PORT, () => console.log('Rockets! Relay on port', PORT));