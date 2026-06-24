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
  res.setHeader("Access-Control-Allow-Origin", "*");

  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", rooms: rooms.size }));
  } else if (req.url?.startsWith("/room/")) {
    // GET /room/:roomId — هل الغرفة موجودة وفيها حد؟
    const roomId = req.url.slice(6).toUpperCase();
    const room = rooms.get(roomId);
    if (room && room.members.size > 0) {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        exists: true,
        members: room.members.size,
        source: room.source || null,
      }));
    } else {
      res.writeHead(404, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ exists: false }));
    }
  } else if (req.url?.startsWith("/yt/")) {
    // GET /yt/:videoId — صفحة YouTube IFrame player
    const videoId = req.url.slice(4).split("?")[0];
    if (!videoId || !/^[a-zA-Z0-9_-]{6,16}$/.test(videoId)) {
      res.writeHead(400);
      res.end("Invalid video ID");
      return;
    }
    const html = `<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<style>
*{margin:0;padding:0;box-sizing:border-box}
html,body{width:100%;height:100%;background:#000;overflow:hidden}
#p{width:100%;height:100%}
</style>
</head>
<body>
<div id="p"></div>
<script>
var tag=document.createElement('script');
tag.src='https://www.youtube.com/iframe_api';
document.head.appendChild(tag);
var player;
function onYouTubeIframeAPIReady(){
  player=new YT.Player('p',{
    videoId:'${videoId}',
    width:'100%',height:'100%',
    playerVars:{autoplay:0,controls:1,rel:0,modestbranding:1,playsinline:1,enablejsapi:1},
    events:{
      onReady:function(e){
        if(window.PlayerBridge) PlayerBridge.postMessage(JSON.stringify({t:'ready',s:0}));
      },
      onStateChange:function(e){
        var s=e.data,ct=player.getCurrentTime()||0;
        if(!window.PlayerBridge) return;
        if(s===1) PlayerBridge.postMessage(JSON.stringify({t:'play',s:ct}));
        else if(s===2) PlayerBridge.postMessage(JSON.stringify({t:'pause',s:ct}));
      }
    }
  });
}
function ytPlay(){if(player)player.playVideo();}
function ytPause(){if(player)player.pauseVideo();}
function ytSeek(t){if(player)player.seekTo(t,true);}
</script>
</body>
</html>`;
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(html);
  } else {
    res.writeHead(200);
    res.end("🎬 WatchParty Server is running!");
  }
});

// ── WebSocket Server ──────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer });

// rooms: Map<roomId, { members, isPlaying, currentTime, lastUpdate, source, voiceUids, deleteTimer }>
const rooms = new Map();

// buffer للـ ICE candidates اللي وصلت قبل الـ offer/answer
// Map<"roomId:from:to", candidate[]>
const iceBuffer = new Map();

function getIceKey(roomId, from, to) {
  return `${roomId}:${from}:${to}`;
}

function bufferIce(roomId, from, to, candidate) {
  const key = getIceKey(roomId, from, to);
  if (!iceBuffer.has(key)) iceBuffer.set(key, []);
  iceBuffer.get(key).push(candidate);
  // امسح الـ buffer بعد 30 ثانية لو ماتم استخدامه
  setTimeout(() => iceBuffer.delete(key), 30000);
}

function flushIce(roomId, from, to, ws) {
  const key = getIceKey(roomId, from, to);
  const candidates = iceBuffer.get(key) || [];
  iceBuffer.delete(key);
  for (const c of candidates) {
    if (ws.readyState === 1) ws.send(JSON.stringify(c));
  }
}

const ROOM_TTL_MS = 5 * 60 * 1000; // 5 دقايق قبل ما الغرفة الفاضية تتمسح

function scheduleRoomDelete(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  if (room.deleteTimer) clearTimeout(room.deleteTimer);
  room.deleteTimer = setTimeout(() => {
    const r = rooms.get(roomId);
    if (r && r.members.size === 0) {
      rooms.delete(roomId);
      console.log(`[🗑] Room ${roomId} deleted after TTL`);
    }
  }, ROOM_TTL_MS);
}

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
      case "set_source":
        handleSetSource(ws, msg);
        break;
      case "voice_join":
        handleVoiceJoin(ws, msg);
        break;
      case "voice_leave":
        handleVoiceLeave(ws, msg);
        break;
      // WebRTC signaling — بس بيتبعت للـ peer المحدد
      case "voice_offer":
      case "voice_answer":
      case "voice_ice":
        handleVoiceSignal(ws, msg);
        break;
      case "voice_mute":
        handleVoiceSignal(ws, msg);
        break;
      case "pdf_page":
        handlePdfPage(ws, msg);
        break;
      case "ping":
        // keep-alive من الـ client — نتجاهله بهدوء
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
          // مش بنمسح فوراً — بنستنى 5 دقايق للـ reconnection
          scheduleRoomDelete(userRoom);
          console.log(`[⏳] Room ${userRoom} empty, scheduled for deletion`);
        } else {
          // شيل من الـ voice لو كان فيها
          if (room.voiceUids) {
            room.voiceUids.delete(userUsername);
            broadcastToRoom(userRoom, null, {
              type: "voice_state",
              voiceUids: [...room.voiceUids]
            });
          }
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

    // إلغاء timer المسح لو كان مجدول
    if (room.deleteTimer) {
      clearTimeout(room.deleteTimer);
      room.deleteTimer = null;
    }

    console.log(`[✅] ${username} joined room ${roomId}. Members: ${room.members.size}`);

    // ابعت للمنضم الجديد حالة الغرفة الحالية (+ مصدر الفيديو)
    send(ws, {
      type: "room_state",
      roomId,
      isPlaying: room.isPlaying,
      currentTime: getAdjustedTime(room),
      memberCount: room.members.size,
      source: room.source || null,
      currentPdfPage: room.currentPdfPage || null
    });

    // لو الغرفة عندها source، ابعته كمان كـ source message منفردة
    if (room.source) {
      send(ws, { type: "source", source: room.source });
    }

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

    // ابعت للباقيين بس (المرسل شايفها بالفعل)
    broadcastToRoom(userRoom, ws, chatMsg);
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

  // ── Voice Join ──────────────────────────────────────────────
  function handleVoiceJoin(ws, msg) {
    const room = getRoomOf(ws);
    if (!room) return;
    if (!room.voiceUids) room.voiceUids = new Set();
    room.voiceUids.add(userUsername);
    console.log(`[🎙] ${userUsername} joined voice in room ${userRoom}`);
    broadcastToRoom(userRoom, null, {
      type: "voice_state",
      voiceUids: [...room.voiceUids]
    });
  }

  // ── Voice Leave ─────────────────────────────────────────────
  function handleVoiceLeave(ws, msg) {
    const room = getRoomOf(ws);
    if (!room) return;
    if (room.voiceUids) room.voiceUids.delete(userUsername);
    console.log(`[🔇] ${userUsername} left voice in room ${userRoom}`);
    broadcastToRoom(userRoom, null, {
      type: "voice_state",
      voiceUids: room.voiceUids ? [...room.voiceUids] : []
    });
  }

  // ── Voice Signaling (WebRTC offer/answer/ice) ───────────────
  function handleVoiceSignal(ws, msg) {
    const room = getRoomOf(ws);
    if (!room) return;

    // voice_mute يتبعت لكل الأعضاء التانيين
    if (msg.type === "voice_mute") {
      const outMsg = { ...msg, from: userUsername };
      room.members.forEach((info, memberWs) => {
        if (memberWs !== ws && memberWs.readyState === 1) {
          memberWs.send(JSON.stringify(outMsg));
        }
      });
      return;
    }

    if (!msg.to) return;

    const outMsg = { ...msg, from: userUsername };

    // لو offer أو answer، ابعته وبعدين flush أي ICE كان متبفر
    if (msg.type === "voice_offer" || msg.type === "voice_answer") {
      room.members.forEach((info, memberWs) => {
        if (info.username === msg.to && memberWs.readyState === 1) {
          memberWs.send(JSON.stringify(outMsg));
          // flush الـ ICE candidates المتبفرة
          flushIce(userRoom, userUsername, msg.to, memberWs);
        }
      });
      return;
    }

    // لو ice candidate، ابعته لو الـ peer موجود، وإلا buffer
    let sent = false;
    room.members.forEach((info, memberWs) => {
      if (info.username === msg.to && memberWs.readyState === 1) {
        memberWs.send(JSON.stringify(outMsg));
        sent = true;
      }
    });
    if (!sent) {
      // الـ peer ما وصلتوش الـ offer بعد، نبفر الـ ICE
      bufferIce(userRoom, userUsername, msg.to, outMsg);
    }
  }

  // ── PDF Page sync ────────────────────────────────────────────
  function handlePdfPage(ws, msg) {
    if (msg.page == null) return;
    const room = getRoomOf(ws);
    if (!room) return;
    room.currentPdfPage = msg.page;
    broadcastToRoom(userRoom, ws, { type: "pdf_page", page: msg.page });
  }

  // ── Set Source (مصدر الفيديو — خاص بالموقع/webapp) ──────────
  function handleSetSource(ws, msg) {
    const room = getRoomOf(ws);
    if (!room || !msg.source) return;
    room.source = msg.source;
    console.log(`[🎞] source set in room ${userRoom}: ${msg.source.type}`);
    // أبلغ الباقيين بالمصدر الجديد
    broadcastToRoom(userRoom, ws, { type: "source", source: msg.source });
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
