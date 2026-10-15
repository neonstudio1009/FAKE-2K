const http = require('http');
const { WebSocketServer } = require('ws');

// 只允許你的 GitHub Pages 連線。多個網址用逗號分開,沒設定就不限制。
const ALLOWED = (process.env.ALLOWED_ORIGINS || '')
  .split(',').map(s => s.trim()).filter(Boolean);

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.end('ok');
});

const wss = new WebSocketServer({
  server,
  path: '/ws',
  maxPayload: 16 * 1024,
  verifyClient: (info) => ALLOWED.length === 0 || ALLOWED.includes(info.origin),
});

const rooms = new Map();
const sendJson = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };

function leave(ws) {
  const r = ws.room;
  if (!r) return;
  rooms.delete(r.code);
  const other = r.host === ws ? r.guest : r.host;
  if (other) { other.room = null; sendJson(other, { t: 'peer_left' }); }
  ws.room = null;
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.room = null;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    const text = data.toString();
    let m;
    try { m = JSON.parse(text); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'ping') return;

    if (m.t === 'create') {
      leave(ws);
      let c;
      do { c = String(1000 + Math.floor(Math.random() * 9000)); } while (rooms.has(c));
      const room = { code: c, host: ws, guest: null };
      rooms.set(c, room);
      ws.room = room;
      sendJson(ws, { t: 'created', code: c });
      return;
    }

    if (m.t === 'join') {
      const room = rooms.get(String(m.code));
      if (!room || room.guest || room.host === ws) {
        sendJson(ws, { t: 'err', msg: '找不到房間,或房間已滿' });
        return;
      }
      room.guest = ws;
      ws.room = room;
      sendJson(ws, { t: 'joined' });
      sendJson(room.host, { t: 'peer_join' });
      return;
    }

    const r = ws.room;
    if (!r) return;
    const other = r.host === ws ? r.guest : r.host;
    if (other && other.readyState === 1) other.send(text);
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => {});
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

const PORT = process.env.PORT || 10000;
server.listen(PORT, '0.0.0.0', () => console.log('listening on ' + PORT));
