const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || ;
const wss = new WebSocketServer({ port: PORT 25440});

let nextId = 1;
const clients = new Map(); // ws -> { id, name }
const vars = new Map(); // shared variables

const send = (ws, text) => ws.readyState === 1 && ws.send(text);
const broadcast = (text, except) => {
  for (const ws of clients.keys()) if (ws !== except) send(ws, text);
};
const findByName = (name) =>
  [...clients.entries()].find(([, c]) => c.name === name);
const sendUsers = () =>
  broadcast(
    "USERS|" +
      [...clients.values()].filter((c) => c.name).map((c) => c.name).join(",")
  );

wss.on("connection", (ws) => {
  const me = { id: nextId++, name: "" };
  clients.set(ws, me);
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));

  // send current shared variables to the new player
  for (const [k, v] of vars) send(ws, `VAR|${k}|${v}`);

  ws.on("message", (data) => {
    const text = data.toString();
    const [cmd, ...rest] = text.split("|");

    switch (cmd) {
      case "NAME": {
        const name = (rest[0] || "").trim();
        if (!name || name.length > 20 || name.includes(","))
          return send(ws, "ERROR|BAD_NAME");
        if (findByName(name)) return send(ws, "ERROR|NAME_TAKEN");
        me.name = name;
        send(ws, `ID|${me.id}`);
        sendUsers();
        break;
      }
      case "ALL": // ALL|message
        if (!me.name) return send(ws, "ERROR|NAME_REQUIRED");
        broadcast(`MSG|${me.name}|${rest.join("|")}`, ws);
        break;
      case "TO": { // TO|name|message
        if (!me.name) return send(ws, "ERROR|NAME_REQUIRED");
        const t = findByName(rest[0]);
        if (!t) return send(ws, "ERROR|USER_NOT_FOUND");
        send(t[0], `DM|${me.name}|${rest.slice(1).join("|")}`);
        break;
      }
      case "SET": { // SET|key|value
        if (!me.name) return send(ws, "ERROR|NAME_REQUIRED");
        const key = rest[0];
        const value = rest.slice(1).join("|");
        vars.set(key, value);
        broadcast(`VAR|${key}|${value}`, ws);
        break;
      }
      case "PING":
        send(ws, "PONG");
        break;
      default:
        send(ws, "ERROR|UNKNOWN_COMMAND");
    }
  });

  ws.on("close", () => {
    clients.delete(ws);
    sendUsers();
  });
});

// drop dead connections every 30 seconds
setInterval(() => {
  for (const ws of clients.keys()) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

console.log(`Server running on ws://localhost:${PORT}`);
