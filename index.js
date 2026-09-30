// Entry point. Every WebSocket connection is routed to ONE Durable Object
// instance ("the hub") that holds every active room in memory. This sidesteps
// a real constraint of this protocol: the client connects to a bare URL and
// only tells the server which room it wants *after* the socket is open (see
// CREATE_ROOM / JOIN_ROOM in Protocol.kt) — so there's no room code available
// at HTTP-upgrade time to route by. A single hub avoids ever needing to hand
// an already-accepted WebSocket off to a different Durable Object (which
// isn't possible). It uses the WebSocket Hibernation API, so idle
// connections cost nothing while parked.
export { ListenTogetherHub } from "./hub.js";

export default {
  async fetch(request, env) {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== "websocket") {
      return new Response("Jaat Player Listen Together server is running.", {
        status: 200,
      });
    }

    const id = env.LISTEN_TOGETHER_HUB.idFromName("hub");
    const stub = env.LISTEN_TOGETHER_HUB.get(id);
    return stub.fetch(request);
  },
};
