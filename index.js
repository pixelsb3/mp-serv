const { WebSocketServer } = require("ws");
const { Chess } = require("chess.js");

const PORT = process.env.PORT || 25440;
const wss = new WebSocketServer({ port: PORT });

const games = new Map(); // id -> game
const BLACK = 0;
const WHITE = 1;

const send = (ws, obj) => ws.readyState === 1 && ws.send(JSON.stringify(obj));
const err = (ws, code) => send(ws, { type: "error", code });

function makeId() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let id;
  do {
    id = Array.from({ length: 4 }, () => chars[Math.floor(Math.random() * chars.length)]).join("");
  } while (games.has(id));
  return id;
}

// Piece names follow your schema; used to check the "piece" field of a move.
function startingNames() {
  const n = {};
  for (const f of "abcdefgh") {
    n[f + "2"] = `${f}-pawn`;
    n[f + "7"] = `${f}-pawn`;
  }
  for (const r of ["1", "8"]) {
    n["a" + r] = "k-rook";
    n["h" + r] = "q-rook";
    n["b" + r] = "k-knight";
    n["g" + r] = "q-knight";
    n["c" + r] = "k-bishop";
    n["f" + r] = "q-bishop";
    n["d" + r] = "queen";
    n["e" + r] = "king";
  }
  return n;
}

function gameInfo(g) {
  return {
    id: g.id,
    players: g.players.map((p) => ({
      username: p.username,
      side: p.side,
      opp: g.players.find((o) => o !== p)?.username ?? null,
    })),
    moves: g.moves,
    fen: g.chess.fen(),
    turn: g.chess.turn() === "w" ? WHITE : BLACK,
  };
}

const broadcast = (g, obj) => g.players.forEach((p) => send(p.ws, obj));

function endGame(g, result, reason, winner = null) {
  g.over = true;
  broadcast(g, { type: "gameover", id: g.id, result, reason, winner });
}

wss.on("connection", (ws) => {
  ws.gameId = null;

  ws.on("message", (raw) => {
    const text = raw.toString();

    // PING works as plain text or as JSON
    if (text.trim().toUpperCase() === "PING") return ws.send("PONG");

    let m;
    try {
      m = JSON.parse(text);
    } catch {
      return err(ws, "BAD_JSON");
    }
    if (!m || typeof m.type !== "string") return err(ws, "BAD_MESSAGE");

    // JSON version of ping
    if (m.type.toLowerCase() === "ping") return send(ws, { type: "pong" });

    switch (m.type) {
      case "create": {
        const username = String(m.username ?? "").trim();
        if (!username || username.length > 20) return err(ws, "BAD_USERNAME");
        if (ws.gameId) return err(ws, "ALREADY_IN_GAME");
        const g = {
          id: makeId(),
          chess: new Chess(),
          names: startingNames(),
          players: [{ ws, username, side: WHITE }],
          moves: [],
          over: false,
        };
        games.set(g.id, g);
        ws.gameId = g.id;
        send(ws, { type: "created", id: g.id, side: WHITE });
        break;
      }

      case "join": {
        const username = String(m.username ?? "").trim();
        const g = games.get(String(m.id ?? "").toUpperCase());
        if (!username || username.length > 20) return err(ws, "BAD_USERNAME");
        if (ws.gameId) return err(ws, "ALREADY_IN_GAME");
        if (!g) return err(ws, "GAME_NOT_FOUND");
        if (g.players.length >= 2) return err(ws, "GAME_FULL");
        if (g.players[0].username === username) return err(ws, "USERNAME_TAKEN");
        g.players.push({ ws, username, side: BLACK });
        ws.gameId = g.id;
        broadcast(g, { type: "started", game: gameInfo(g) });
        break;
      }

      case "move": {
        const g = games.get(ws.gameId);
        if (!g || g.players.length < 2) return err(ws, "NO_ACTIVE_GAME");
        if (g.over) return err(ws, "GAME_OVER");
        const me = g.players.find((p) => p.ws === ws);
        const turnSide = g.chess.turn() === "w" ? WHITE : BLACK;
        if (me.side !== turnSide) return err(ws, "NOT_YOUR_TURN");

        const from = m.from, to = m.to;
        const promotion = m.promotion ?? undefined;
        const piece = g.names[from];
        if (m.piece && m.piece !== piece) return err(ws, "PIECE_MISMATCH");

        let result;
        try {
          result = g.chess.move({ from, to, promotion });
        } catch {
          return err(ws, "ILLEGAL_MOVE");
        }

        // keep piece-name tracking in sync
        delete g.names[from];
        if (result.flags.includes("e")) delete g.names[to[0] + from[1]]; // en passant
        g.names[to] = piece;
        if (result.flags.includes("k")) { // kingside castle
          const r = from[1];
          g.names["f" + r] = g.names["h" + r];
          delete g.names["h" + r];
        }
        if (result.flags.includes("q")) { // queenside castle
          const r = from[1];
          g.names["d" + r] = g.names["a" + r];
          delete g.names["a" + r];
        }

        const record = {
          side: me.side,
          from,
          to,
          piece,
          promotion: promotion ?? null,
          captured: result.captured ?? null,
          san: result.san,
        };
        g.moves.push(record);

        broadcast(g, {
          type: "move",
          id: g.id,
          move: record,
          fen: g.chess.fen(),
          check: g.chess.isCheck(),
        });

        if (g.chess.isCheckmate()) endGame(g, "win", "checkmate", me.side);
        else if (g.chess.isStalemate()) endGame(g, "draw", "stalemate");
        else if (g.chess.isInsufficientMaterial()) endGame(g, "draw", "insufficient_material");
        else if (g.chess.isThreefoldRepetition()) endGame(g, "draw", "repetition");
        else if (g.chess.isDraw()) endGame(g, "draw", "fifty_move_rule");
        break;
      }

      case "resign": {
        const g = games.get(ws.gameId);
        if (!g || g.over || g.players.length < 2) return err(ws, "NO_ACTIVE_GAME");
        const me = g.players.find((p) => p.ws === ws);
        endGame(g, "win", "resignation", me.side === WHITE ? BLACK : WHITE);
        break;
      }

      case "sync": { // get the full game state
        const g = games.get(ws.gameId);
        if (!g) return err(ws, "NO_ACTIVE_GAME");
        send(ws, { type: "sync", game: gameInfo(g) });
        break;
      }

      default:
        err(ws, "UNKNOWN_TYPE");
    }
  });

  ws.on("close", () => {
    const g = games.get(ws.gameId);
    if (!g) return;
    g.players = g.players.filter((p) => p.ws !== ws);
    if (g.players.length === 0) return games.delete(g.id);
    if (!g.over && g.chess.history().length > 0) {
      endGame(g, "win", "opponent_left", g.players[0].side);
    } else {
      broadcast(g, { type: "opponent_left", id: g.id });
    }
  });
});

console.log(`Cloud Multiplayer server running on ws://localhost:${PORT}`);
