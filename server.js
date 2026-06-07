// ============================================
//  WatchParty — server.js
//  Node.js + ws (WebSocket)
//  تشغيل: node server.js
// ============================================

const { WebSocketServer } = require("ws");
const http = require("http");

const PORT = process.env.PORT || 3000;

// ── HTTP Server (لازم لـ Render و Railway) ───────────────────
const httpServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200);
    res.end(JSON.stringify({ status: "ok", rooms: rooms.size }));
  } else {
    res.writeHead(200);
    res.end("🎬 WatchParty Server is running!");
  }
});

// ── WebSocket Server ──────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer });

// rooms: Map<roomId, { members: Map<ws, { username, joinedAt }>, isPlaying, currentTime, lastUpdate }>
const rooms = new Map();

// ── Connection Handler ────────────────────────────────────────
wss.on("connection", (ws) => {
  let userRoom = null;
  let userUsername = null;

  console.log(`[+] New connection. Total: ${wss.clients.size}`);

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }

    switch (msg.type) {
      case "join":
        handleJoin(ws, msg);
        break;
      case "play":
        handlePlay(ws, msg);
        break;
      case "pause":
        handlePause(ws, msg);
        break;
      case "seek":
        handleSeek(ws, msg);
        break;
      case "chat":
        handleChat(ws, msg);
        break;
      case "reaction":
        handleReaction(ws, msg);
        break;
    }
  });

  ws.on("close", () => {
    if (userRoom && userUsername) {
      const room = rooms.get(userRoom);
      if (room) {
        room.members.delete(ws);
        console.log(`[-] ${userUsername} left room ${userRoom}. Members: ${room.members.size}`);

        if (room.members.size === 0) {
          rooms.delete(userRoom);
          console.log(`[🗑] Room ${userRoom} deleted (empty)`);
        } else {
          // أبلغ الباقيين
          broadcastToRoom(userRoom, ws, {
            type: "user_left",
            username: userUsername
          });
          broadcastMembersList(userRoom);
        }
      }
    }
  });

  // ── Join ────────────────────────────────────────────────────
  function handleJoin(ws, msg) {
    const { roomId, username } = msg;
    if (!roomId || !username) return;

    userRoom = roomId;
    userUsername = username;

    // إنشاء الغرفة لو مش موجودة
    if (!rooms.has(roomId)) {
      rooms.set(roomId, {
        members: new Map(),
        isPlaying: false,
        currentTime: 0,
        lastUpdate: Date.now()
      });
      console.log(`[🆕] Room ${roomId} created`);
    }

    const room = rooms.get(roomId);
    room.members.set(ws, { username, joinedAt: Date.now() });

    console.log(`[✅] ${username} joined room ${roomId}. Members: ${room.members.size}`);

    // ابعت للمنضم الجديد حالة الغرفة الحالية
    send(ws, {
      type: "room_state",
      roomId,
      isPlaying: room.isPlaying,
      currentTime: getAdjustedTime(room),
      memberCount: room.members.size
    });

    // أبلغ الباقيين
    broadcastToRoom(roomId, ws, {
      type: "user_joined",
      username
    });

    // ابعت قائمة الأعضاء للكل
    broadcastMembersList(roomId);
  }

  // ── Play ────────────────────────────────────────────────────
  function handlePlay(ws, msg) {
    const room = getRoomOf(ws);
    if (!room) return;

    room.isPlaying = true;
    room.currentTime = msg.time || 0;
    room.lastUpdate = Date.now();

    console.log(`[▶] ${userUsername} played at ${msg.time?.toFixed(1)}s in room ${userRoom}`);

    broadcastToRoom(userRoom, ws, {
      type: "sync",
      action: "play",
      time: msg.time,
      by: userUsername
    });
  }

  // ── Pause ───────────────────────────────────────────────────
  function handlePause(ws, msg) {
    const room = getRoomOf(ws);
    if (!room) return;

    room.isPlaying = false;
    room.currentTime = msg.time || 0;
    room.lastUpdate = Date.now();

    console.log(`[⏸] ${userUsername} paused at ${msg.time?.toFixed(1)}s in room ${userRoom}`);

    broadcastToRoom(userRoom, ws, {
      type: "sync",
      action: "pause",
      time: msg.time,
      by: userUsername
    });
  }

  // ── Seek ────────────────────────────────────────────────────
  function handleSeek(ws, msg) {
    const room = getRoomOf(ws);
    if (!room) return;

    room.currentTime = msg.time || 0;
    room.lastUpdate = Date.now();

    console.log(`[⏩] ${userUsername} seeked to ${msg.time?.toFixed(1)}s in room ${userRoom}`);

    broadcastToRoom(userRoom, ws, {
      type: "sync",
      action: "seek",
      time: msg.time,
      by: userUsername
    });
  }

  // ── Chat ────────────────────────────────────────────────────
  function handleChat(ws, msg) {
    if (!msg.text || !msg.text.trim()) return;
    const room = getRoomOf(ws);
    if (!room) return;

    const chatMsg = {
      type: "chat",
      username: userUsername,
      text: msg.text.trim().substring(0, 300), // حد 300 حرف
      timestamp: Date.now()
    };

    console.log(`[💬] ${userUsername} in ${userRoom}: ${chatMsg.text}`);

    // ابعت للكل (بما فيهم المرسل)
    broadcastToRoom(userRoom, null, chatMsg);
  }

  // ── Reaction ────────────────────────────────────────────────
  function handleReaction(ws, msg) {
    if (!msg.emoji) return;
    const room = getRoomOf(ws);
    if (!room) return;

    // ابعت لباقي الأعضاء (مش للمرسل، عشان هو شافها بالفعل)
    broadcastToRoom(userRoom, ws, {
      type: "reaction",
      username: userUsername,
      emoji: String(msg.emoji).substring(0, 8)
    });
  }

  // ── Helpers ─────────────────────────────────────────────────
  function getRoomOf(ws) {
    if (!userRoom) return null;
    return rooms.get(userRoom) || null;
  }
});

// ── Broadcast to all in room (except optional excluded ws) ────
function broadcastToRoom(roomId, excludeWs, data) {
  const room = rooms.get(roomId);
  if (!room) return;

  const json = JSON.stringify(data);
  room.members.forEach((info, ws) => {
    if (ws !== excludeWs && ws.readyState === 1 /* OPEN */) {
      ws.send(json);
    }
  });
}

// ── ابعت قائمة الأعضاء للكل ─────────────────────────────────
function broadcastMembersList(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;

  const members = [...room.members.values()].map(m => m.username);
  const json = JSON.stringify({ type: "members", members });

  room.members.forEach((info, ws) => {
    if (ws.readyState === 1) ws.send(json);
  });
}

// ── احسب الوقت الحالي مع حساب الـ elapsed time ───────────────
function getAdjustedTime(room) {
  if (!room.isPlaying) return room.currentTime;
  const elapsed = (Date.now() - room.lastUpdate) / 1000;
  return room.currentTime + elapsed;
}

// ── Send helper ───────────────────────────────────────────────
function send(ws, data) {
  if (ws.readyState === 1) {
    ws.send(JSON.stringify(data));
  }
}

// ── Start ─────────────────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log(`\n🎬 WatchParty Server running on port ${PORT}`);
  console.log(`📡 WebSocket: ws://localhost:${PORT}`);
  console.log(`🏥 Health: http://localhost:${PORT}/health\n`);
});
