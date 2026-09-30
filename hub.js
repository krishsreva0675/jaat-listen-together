import { DurableObject } from "cloudflare:workers";

// Must stay a bit below the client's own SESSION_GRACE_PERIOD_MS (10 min, see
// ListenTogetherClient.kt) so the server never expires a session the client
// still thinks is valid.
const GRACE_PERIOD_MS = 10 * 60 * 1000;
const ROOM_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // no 0/O/1/I mix-ups

function genId() {
  return crypto.randomUUID();
}

function genRoomCode(existingCodes) {
  let code;
  do {
    code = Array.from(
      { length: 6 },
      () => ROOM_CODE_CHARS[Math.floor(Math.random() * ROOM_CODE_CHARS.length)]
    ).join("");
  } while (existingCodes.has(code));
  return code;
}

export class ListenTogetherHub extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.env = env;
    /** @type {Map<string, any>} roomCode -> room */
    this.rooms = new Map();
    this.loaded = false;
  }

  async ensureLoaded() {
    if (this.loaded) return;
    const stored = (await this.ctx.storage.get("rooms")) || {};
    for (const [code, room] of Object.entries(stored)) {
      this.rooms.set(code, room);
    }
    this.loaded = true;
  }

  async persist() {
    await this.ctx.storage.put("rooms", Object.fromEntries(this.rooms));
  }

  // ---------------------------------------------------------------- fetch --

  async fetch(request) {
    await this.ensureLoaded();
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  // ---------------------------------------------------- hibernation events --

  async webSocketMessage(ws, message) {
    await this.ensureLoaded();
    let msg;
    try {
      msg = JSON.parse(typeof message === "string" ? message : new TextDecoder().decode(message));
    } catch {
      return;
    }
    try {
      await this.handleMessage(ws, msg);
    } catch (err) {
      this.sendTo(ws, "error", { code: "internal_error", message: String((err && err.message) || err) });
    }
  }

  async webSocketClose(ws) {
    await this.ensureLoaded();
    await this.handleDisconnect(ws);
  }

  async webSocketError(ws) {
    await this.ensureLoaded();
    await this.handleDisconnect(ws);
  }

  async alarm() {
    await this.ensureLoaded();
    const now = Date.now();
    let anyPendingExpiry = false;

    for (const [code, room] of [...this.rooms]) {
      let changed = false;
      for (const [userId, user] of Object.entries(room.users)) {
        if (user.disconnectedAt && now - user.disconnectedAt >= GRACE_PERIOD_MS) {
          delete room.users[userId];
          changed = true;
          this.broadcast(code, "user_left", { user_id: userId, username: user.username });
          if (room.hostId === userId) {
            this.reassignHost(room);
          }
        } else if (user.disconnectedAt) {
          anyPendingExpiry = true;
        }
      }
      if (Object.keys(room.users).length === 0) {
        this.rooms.delete(code);
        changed = true;
        continue;
      }
      if (changed) this.touchRoom(room);
    }

    await this.persist();
    if (anyPendingExpiry) {
      await this.ctx.storage.setAlarm(now + 30_000);
    }
  }

  scheduleCleanupCheck() {
    this.ctx.storage.getAlarm().then((existing) => {
      if (!existing) this.ctx.storage.setAlarm(Date.now() + 30_000);
    });
  }

  // ------------------------------------------------------------- helpers --

  attachmentFor(ws) {
    try {
      return ws.deserializeAttachment() || null;
    } catch {
      return null;
    }
  }

  socketsForRoom(roomCode) {
    return this.ctx.getWebSockets().filter((ws) => {
      const a = this.attachmentFor(ws);
      return a && a.roomCode === roomCode;
    });
  }

  socketForUser(roomCode, userId) {
    return this.socketsForRoom(roomCode).find((ws) => this.attachmentFor(ws)?.userId === userId) || null;
  }

  sendTo(ws, type, payload) {
    try {
      ws.send(JSON.stringify({ type, payload: payload ?? null }));
    } catch {
      // socket already gone; ignore
    }
  }

  sendToUser(roomCode, userId, type, payload) {
    const ws = this.socketForUser(roomCode, userId);
    if (ws) this.sendTo(ws, type, payload);
  }

  broadcast(roomCode, type, payload, excludeUserId) {
    for (const ws of this.socketsForRoom(roomCode)) {
      const a = this.attachmentFor(ws);
      if (a && a.userId !== excludeUserId) this.sendTo(ws, type, payload);
    }
  }

  touchRoom(room) {
    room.lastUpdate = Date.now();
  }

  publicState(room) {
    return {
      room_code: room.roomCode,
      host_id: room.hostId,
      users: Object.values(room.users).map((u) => ({
        user_id: u.userId,
        username: u.username,
        is_host: u.userId === room.hostId,
        is_connected: !u.disconnectedAt,
      })),
      current_track: room.currentTrack,
      is_playing: room.isPlaying,
      position: this.extrapolatedPosition(room),
      last_update: Date.now(),
      volume: room.volume,
      queue: room.queue,
      allow_participant_control: room.allowParticipantControl,
    };
  }

  extrapolatedPosition(room) {
    if (!room.isPlaying) return room.position;
    return room.position + (Date.now() - room.lastUpdate);
  }

  reassignHost(room) {
    const remaining = Object.values(room.users).filter((u) => !u.disconnectedAt);
    if (remaining.length === 0) return;
    const next = remaining[0];
    room.hostId = next.userId;
    this.broadcast(room.roomCode, "host_changed", {
      new_host_id: next.userId,
      new_host_name: next.username,
    });
  }

  requireRoom(ws) {
    const a = this.attachmentFor(ws);
    if (!a || !a.roomCode) return null;
    return this.rooms.get(a.roomCode) || null;
  }

  isHost(room, userId) {
    return room.hostId === userId;
  }

  canControlPlayback(room, userId) {
    return this.isHost(room, userId) || room.allowParticipantControl === true;
  }

  // -------------------------------------------------------- message router --

  async handleMessage(ws, msg) {
    const { type, payload } = msg;
    switch (type) {
      case "create_room":
        return this.onCreateRoom(ws, payload);
      case "join_room":
        return this.onJoinRoom(ws, payload);
      case "leave_room":
        return this.onLeaveRoom(ws);
      case "approve_join":
        return this.onApproveJoin(ws, payload);
      case "reject_join":
        return this.onRejectJoin(ws, payload);
      case "playback_action":
        return this.onPlaybackAction(ws, payload);
      case "buffer_ready":
        return this.onBufferReady(ws, payload);
      case "kick_user":
        return this.onKickUser(ws, payload);
      case "transfer_host":
        return this.onTransferHost(ws, payload);
      case "ping":
        return this.sendTo(ws, "pong", null);
      case "chat":
        return this.onChat(ws, payload);
      case "request_sync":
        return this.onRequestSync(ws);
      case "reconnect":
        return this.onReconnect(ws, payload);
      case "suggest_track":
        return this.onSuggestTrack(ws, payload);
      case "approve_suggestion":
        return this.onApproveSuggestion(ws, payload);
      case "reject_suggestion":
        return this.onRejectSuggestion(ws, payload);
      case "update_room_settings":
        return this.onUpdateRoomSettings(ws, payload);
      default:
        this.sendTo(ws, "error", { code: "unknown_type", message: `Unknown message type: ${type}` });
    }
  }

  // ------------------------------------------------------------- handlers --

  async onCreateRoom(ws, payload) {
    const username = String(payload?.username || "Host").slice(0, 40);
    const roomCode = genRoomCode(new Set(this.rooms.keys()));
    const userId = genId();
    const sessionToken = genId();
    const now = Date.now();

    const room = {
      roomCode,
      hostId: userId,
      users: {
        [userId]: { userId, username, sessionToken, disconnectedAt: null },
      },
      currentTrack: null,
      isPlaying: false,
      position: 0,
      lastUpdate: now,
      volume: 1,
      queue: [],
      allowParticipantControl: false,
      pendingJoins: {},
      suggestions: {},
      bufferingFor: null,
      createdAt: now,
    };
    this.rooms.set(roomCode, room);
    ws.serializeAttachment({ roomCode, userId, sessionToken });
    await this.persist();

    this.sendTo(ws, "room_created", { room_code: roomCode, user_id: userId, session_token: sessionToken });
  }

  async onJoinRoom(ws, payload) {
    const roomCode = String(payload?.room_code || "").toUpperCase();
    const username = String(payload?.username || "Guest").slice(0, 40);
    const room = this.rooms.get(roomCode);
    if (!room) {
      return this.sendTo(ws, "error", { code: "room_not_found", message: "That room code doesn't exist." });
    }

    const userId = genId();
    const sessionToken = genId();
    room.pendingJoins[userId] = { username, sessionToken };
    ws.serializeAttachment({ roomCode, userId, sessionToken, pending: true });
    await this.persist();

    this.sendToUser(roomCode, room.hostId, "join_request", { user_id: userId, username });
  }

  async onApproveJoin(ws, payload) {
    const room = this.requireRoom(ws);
    const a = this.attachmentFor(ws);
    if (!room || !a) return;
    if (!this.isHost(room, a.userId)) return this.sendTo(ws, "error", { code: "not_host", message: "Only the host can approve joins." });

    const userId = String(payload?.user_id || "");
    const pending = room.pendingJoins[userId];
    if (!pending) return;
    delete room.pendingJoins[userId];

    room.users[userId] = { userId, username: pending.username, sessionToken: pending.sessionToken, disconnectedAt: null };
    await this.persist();

    const joinerWs = this.socketForUser(room.roomCode, userId);
    if (joinerWs) {
      const attachment = this.attachmentFor(joinerWs) || {};
      joinerWs.serializeAttachment({ ...attachment, pending: false });
    }

    this.sendToUser(room.roomCode, userId, "join_approved", {
      room_code: room.roomCode,
      user_id: userId,
      session_token: pending.sessionToken,
      state: this.publicState(room),
    });
    this.broadcast(room.roomCode, "user_joined", { user_id: userId, username: pending.username }, userId);
  }

  async onRejectJoin(ws, payload) {
    const room = this.requireRoom(ws);
    const a = this.attachmentFor(ws);
    if (!room || !a) return;
    if (!this.isHost(room, a.userId)) return;

    const userId = String(payload?.user_id || "");
    const pending = room.pendingJoins[userId];
    if (!pending) return;
    delete room.pendingJoins[userId];
    await this.persist();

    this.sendToUser(room.roomCode, userId, "join_rejected", { reason: payload?.reason || "Request declined" });
    const joinerWs = this.socketForUser(room.roomCode, userId);
    if (joinerWs) joinerWs.close(4000, "join_rejected");
  }

  async onLeaveRoom(ws) {
    const room = this.requireRoom(ws);
    const a = this.attachmentFor(ws);
    if (!room || !a) return;
    delete room.users[a.userId];

    if (Object.keys(room.users).length === 0) {
      this.rooms.delete(room.roomCode);
    } else {
      if (room.hostId === a.userId) this.reassignHost(room);
      this.broadcast(room.roomCode, "user_left", { user_id: a.userId, username: a.username });
    }
    await this.persist();
  }

  async onPlaybackAction(ws, payload) {
    const room = this.requireRoom(ws);
    const a = this.attachmentFor(ws);
    if (!room || !a) return;
    if (!this.canControlPlayback(room, a.userId)) {
      return this.sendTo(ws, "error", { code: "not_allowed", message: "You don't have playback control in this room." });
    }

    const action = payload?.action;
    const now = Date.now();

    switch (action) {
      case "play":
        room.isPlaying = true;
        if (typeof payload?.position === "number") room.position = payload.position;
        room.lastUpdate = now;
        break;
      case "pause":
        room.position = this.extrapolatedPosition(room);
        room.isPlaying = false;
        room.lastUpdate = now;
        break;
      case "seek":
        room.position = payload?.position ?? room.position;
        room.lastUpdate = now;
        break;
      case "skip_next":
      case "skip_prev":
        room.position = 0;
        room.lastUpdate = now;
        break;
      case "change_track":
        room.currentTrack = payload?.track_info ?? room.currentTrack;
        room.position = payload?.position ?? 0;
        room.lastUpdate = now;
        room.bufferingFor = room.currentTrack ? { trackId: room.currentTrack.id, readyUserIds: [] } : null;
        break;
      case "queue_add":
        if (payload?.track_info) {
          if (payload?.insert_next) room.queue.unshift(payload.track_info);
          else room.queue.push(payload.track_info);
        }
        break;
      case "queue_remove":
        if (payload?.track_id) room.queue = room.queue.filter((t) => t.id !== payload.track_id);
        break;
      case "queue_clear":
        room.queue = [];
        break;
      case "sync_queue":
        if (payload?.queue) room.queue = payload.queue;
        break;
      case "set_volume":
        if (typeof payload?.volume === "number") room.volume = payload.volume;
        break;
      default:
        break;
    }

    await this.persist();
    this.broadcast(room.roomCode, "sync_playback", { ...payload, server_time: now });
  }

  async onBufferReady(ws, payload) {
    const room = this.requireRoom(ws);
    const a = this.attachmentFor(ws);
    if (!room || !a) return;
    const trackId = payload?.track_id;
    if (!trackId || !room.bufferingFor || room.bufferingFor.trackId !== trackId) return;

    if (!room.bufferingFor.readyUserIds.includes(a.userId)) {
      room.bufferingFor.readyUserIds.push(a.userId);
    }
    const connectedUserIds = Object.values(room.users)
      .filter((u) => !u.disconnectedAt)
      .map((u) => u.userId);
    const waitingFor = connectedUserIds.filter((id) => !room.bufferingFor.readyUserIds.includes(id));

    if (waitingFor.length === 0) {
      this.broadcast(room.roomCode, "buffer_complete", { track_id: trackId });
      room.bufferingFor = null;
    } else {
      this.broadcast(room.roomCode, "buffer_wait", { track_id: trackId, waiting_for: waitingFor });
    }
    await this.persist();
  }

  async onKickUser(ws, payload) {
    const room = this.requireRoom(ws);
    const a = this.attachmentFor(ws);
    if (!room || !a) return;
    if (!this.isHost(room, a.userId)) return this.sendTo(ws, "error", { code: "not_host", message: "Only the host can kick." });

    const userId = String(payload?.user_id || "");
    if (!room.users[userId] || userId === a.userId) return;
    delete room.users[userId];
    await this.persist();

    this.sendToUser(room.roomCode, userId, "kicked", { reason: payload?.reason || "Removed by host" });
    const kickedWs = this.socketForUser(room.roomCode, userId);
    if (kickedWs) kickedWs.close(4001, "kicked");
    this.broadcast(room.roomCode, "user_left", { user_id: userId, username: "" });
  }

  async onTransferHost(ws, payload) {
    const room = this.requireRoom(ws);
    const a = this.attachmentFor(ws);
    if (!room || !a) return;
    if (!this.isHost(room, a.userId)) return;

    const newHostId = String(payload?.new_host_id || "");
    const newHost = room.users[newHostId];
    if (!newHost) return;
    room.hostId = newHostId;
    await this.persist();

    this.broadcast(room.roomCode, "host_changed", { new_host_id: newHostId, new_host_name: newHost.username });
  }

  async onChat(ws, payload) {
    const room = this.requireRoom(ws);
    const a = this.attachmentFor(ws);
    if (!room || !a) return;
    const user = room.users[a.userId];
    if (!user) return;

    this.broadcast(room.roomCode, "chat", {
      user_id: a.userId,
      username: user.username,
      message: String(payload?.message || "").slice(0, 2000),
      timestamp: Date.now(),
      reply_to: payload?.reply_to ?? null,
    });
  }

  async onRequestSync(ws) {
    const room = this.requireRoom(ws);
    if (!room) return;
    this.sendTo(ws, "sync_state", {
      current_track: room.currentTrack,
      is_playing: room.isPlaying,
      position: this.extrapolatedPosition(room),
      last_update: Date.now(),
      queue: room.queue,
      volume: room.volume,
    });
  }

  async onReconnect(ws, payload) {
    const sessionToken = String(payload?.session_token || "");
    let found = null;
    for (const room of this.rooms.values()) {
      for (const user of Object.values(room.users)) {
        if (user.sessionToken === sessionToken) {
          found = { room, user };
          break;
        }
      }
      if (found) break;
    }

    if (!found) {
      return this.sendTo(ws, "error", { code: "session_expired", message: "This session is no longer valid." });
    }

    const { room, user } = found;
    user.disconnectedAt = null;
    ws.serializeAttachment({ roomCode: room.roomCode, userId: user.userId, sessionToken });
    await this.persist();

    this.sendTo(ws, "reconnected", {
      room_code: room.roomCode,
      user_id: user.userId,
      state: this.publicState(room),
      is_host: room.hostId === user.userId,
    });
    this.broadcast(room.roomCode, "user_reconnected", { user_id: user.userId, username: user.username }, user.userId);
  }

  async onSuggestTrack(ws, payload) {
    const room = this.requireRoom(ws);
    const a = this.attachmentFor(ws);
    if (!room || !a) return;
    const user = room.users[a.userId];
    if (!user || !payload?.track_info) return;

    const suggestionId = genId();
    room.suggestions[suggestionId] = { fromUserId: a.userId, fromUsername: user.username, trackInfo: payload.track_info };
    await this.persist();

    this.sendToUser(room.roomCode, room.hostId, "suggestion_received", {
      suggestion_id: suggestionId,
      from_user_id: a.userId,
      from_username: user.username,
      track_info: payload.track_info,
    });
  }

  async onApproveSuggestion(ws, payload) {
    const room = this.requireRoom(ws);
    const a = this.attachmentFor(ws);
    if (!room || !a) return;
    if (!this.isHost(room, a.userId)) return;

    const suggestionId = String(payload?.suggestion_id || "");
    const suggestion = room.suggestions[suggestionId];
    if (!suggestion) return;
    delete room.suggestions[suggestionId];
    room.queue.push(suggestion.trackInfo);
    await this.persist();

    this.broadcast(room.roomCode, "suggestion_approved", { suggestion_id: suggestionId, track_info: suggestion.trackInfo });
  }

  async onRejectSuggestion(ws, payload) {
    const room = this.requireRoom(ws);
    const a = this.attachmentFor(ws);
    if (!room || !a) return;
    if (!this.isHost(room, a.userId)) return;

    const suggestionId = String(payload?.suggestion_id || "");
    const suggestion = room.suggestions[suggestionId];
    if (!suggestion) return;
    delete room.suggestions[suggestionId];
    await this.persist();

    this.sendToUser(room.roomCode, suggestion.fromUserId, "suggestion_rejected", {
      suggestion_id: suggestionId,
      reason: payload?.reason ?? null,
    });
  }

  async onUpdateRoomSettings(ws, payload) {
    const room = this.requireRoom(ws);
    const a = this.attachmentFor(ws);
    if (!room || !a) return;
    if (!this.isHost(room, a.userId)) return;

    room.allowParticipantControl = !!payload?.allow_participant_control;
    await this.persist();

    this.broadcast(room.roomCode, "room_settings_changed", { allow_participant_control: room.allowParticipantControl });
  }

  async handleDisconnect(ws) {
    const room = this.requireRoom(ws);
    const a = this.attachmentFor(ws);
    if (!room || !a) return;

    if (a.pending) {
      delete room.pendingJoins[a.userId];
      await this.persist();
      return;
    }

    const user = room.users[a.userId];
    if (!user) return;
    user.disconnectedAt = Date.now();
    await this.persist();

    this.broadcast(room.roomCode, "user_disconnected", { user_id: a.userId, username: user.username }, a.userId);
    this.scheduleCleanupCheck();
  }
}
