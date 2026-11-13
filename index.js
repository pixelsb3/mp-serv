const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 25440;
const wss = new WebSocketServer({ port: PORT });

let nextId = 1;
const clients = new Map(); // ws -> { id, username, rooms:Set }
const DEFAULT_ROOM = "default";

const send = (ws, obj) => ws.readyState === 1 && ws.send(JSON.stringify(obj));

const status = (ws, code, text, listener) =>
  send(ws, {
    cmd: "statuscode",
    val: `${code} | ${text}`,
    code,
    ...(listener !== undefined && { listener }),
  });

const OK = (ws, l) => status(ws, "I:100", "OK", l);
const BAD = (ws, l) => status(ws, "E:101", "Syntax", l);
const NOT_FOUND = (ws, l) => status(ws, "E:103", "ID not found", l);
const NEED_ID = (ws, l) => status(ws, "E:110", "ID required", l);

const userObj = (c) => ({ id: c.id, username: c.username });

const inRoom = (room) =>
  [...clients.entries()].filter(([, c]) => c.rooms.has(room));

function broadcastRoom(room, obj, except) {
  for (const [ws, c] of inRoom(room)) if (ws !== except) send(ws, obj);
}

function broadcastUlist(room) {
  const list = inRoom(room)
    .filter(([, c]) => c.username)
    .map(([, c]) => userObj(c));
  broadcastRoom(room, { cmd: "ulist", val: list });
}

function findUser(target) {
  for (const [ws, c] of clients) {
    if (String(c.id) === String(target) || c.username === target) return [ws, c];
  }
  return null;
}

wss.on("connection", (ws) => {
  const me = { id: nextId++, username: "", rooms: new Set([DEFAULT_ROOM]) };
  clients.set(ws, me);

  send(ws, { cmd: "server_version", val: "node-cl4-1.0" });

  ws.on("message", (raw) => {
    let p;
    try {
      p = JSON.parse(raw.toString());
    } catch {
      return BAD(ws);
    }
    if (!p || typeof p.cmd !== "string") return BAD(ws);
    const l = p.listener;

    switch (p.cmd) {
      case "handshake":
        send(ws, { cmd: "client_obj", val: userObj(me), listener: l });
        OK(ws, l);
        break;

      case "setid": {
        const name = String(p.val ?? "").trim();
        if (!name || name.length > 20) return BAD(ws, l);
        if (me.username) return status(ws, "E:107", "ID already set", l);
        if (findUser(name)) return status(ws, "E:106", "ID in use", l);
        me.username = name;
        send(ws, { cmd: "client_obj", val: userObj(me), listener: l });
        OK(ws, l);
        me.rooms.forEach(broadcastUlist);
        break;
      }

      case "ping":
        OK(ws, l);
        break;

      case "gmsg":
      case "gvar": {
        if (!me.username) return NEED_ID(ws, l);
        const rooms = p.rooms ? [].concat(p.rooms) : [...me.rooms];
        for (const r of rooms) {
          if (!me.rooms.has(r)) continue;
          const out = { cmd: p.cmd, val: p.val, origin: userObj(me), rooms: r };
          if (p.cmd === "gvar") out.name = p.name;
          broadcastRoom(r, out, ws);
        }
        OK(ws, l);
        break;
      }

      case "pmsg":
      case "pvar": {
        if (!me.username) return NEED_ID(ws, l);
        const t = findUser(p.id);
        if (!t) return NOT_FOUND(ws, l);
        const out = { cmd: p.cmd, val: p.val, origin: userObj(me) };
        if (p.cmd === "pvar") out.name = p.name;
        send(t[0], out);
        OK(ws, l);
        break;
      }

      case "direct": {
        if (!me.username) return NEED_ID(ws, l);
        const t = p.id ? findUser(p.id) : null;
        if (!t) return NOT_FOUND(ws, l);
        send(t[0], { cmd: "direct", val: p.val, origin: userObj(me) });
        OK(ws, l);
        break;
      }

      case "link": {
        const rooms = [].concat(p.val ?? []).map(String);
        if (!rooms.length) return BAD(ws, l);
        me.rooms.delete(DEFAULT_ROOM);
        rooms.forEach((r) => me.rooms.add(r));
        OK(ws, l);
        rooms.forEach(broadcastUlist);
        break;
      }

      case "unlink": {
        const old = [...me.rooms];
        me.rooms = new Set([DEFAULT_ROOM]);
        OK(ws, l);
        old.forEach(broadcastUlist);
        broadcastUlist(DEFAULT_ROOM);
        break;
      }

      default:
        status(ws, "E:109", "Invalid command", l);
    }
  });

  ws.on("close", () => {
    const rooms = [...me.rooms];
    clients.delete(ws);
    rooms.forEach(broadcastUlist);
  });
});

console.log(`CloudLink-style server running on ws://localhost:${PORT}`);
