import { connect } from 'cloudflare:sockets';

const UUID = '00000000-0000-4000-8000-000000000000';

export default {
  async fetch(request, env) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('OK', { status: 200 });
    }
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    handle(server, env.UUID || UUID);
    return new Response(null, { status: 101, webSocket: client });
  }
};

function handle(ws, uuid) {
  let remote = null, writer = null, first = true;

  async function onMsg(e) {
    try {
      const data = new Uint8Array(
        e.data instanceof ArrayBuffer ? e.data : await e.data.arrayBuffer()
      );
      if (!first) { await writer.write(data); return; }
      first = false;

      const id = [...data.slice(1, 17)]
        .map(b => b.toString(16).padStart(2, '0')).join('');
      if (id !== uuid.replace(/-/g, '').toLowerCase()) return ws.close();

      let i = 18 + data[17];
      const cmd = data[i++];
      const port = (data[i] << 8) | data[i + 1]; i += 2;
      const atype = data[i++];
      let host;
      if (atype === 1) { host = data.slice(i, i + 4).join('.'); i += 4; }
      else if (atype === 2) {
        const l = data[i++];
        host = new TextDecoder().decode(data.slice(i, i + l)); i += l;
      } else if (atype === 3) {
        const p = [];
        for (let k = 0; k < 8; k++) p.push(((data[i + k * 2] << 8) | data[i + k * 2 + 1]).toString(16));
        host = p.join(':'); i += 16;
      } else return ws.close();
      if (cmd !== 1) return ws.close();

      remote = connect({ hostname: host, port });
      writer = remote.writable.getWriter();
      await writer.write(data.slice(i));

      let header = new Uint8Array([data[0], 0]);
      remote.readable.pipeTo(new WritableStream({
        write(chunk) {
          if (header) {
            const m = new Uint8Array(header.length + chunk.length);
            m.set(header); m.set(chunk, header.length);
            ws.send(m); header = null;
          } else ws.send(chunk);
        },
        close() { ws.close(); },
        abort() { ws.close(); }
      })).catch(() => ws.close());
    } catch { ws.close(); }
  }

  let chain = Promise.resolve();
  ws.addEventListener('message', (e) => {
    chain = chain.then(() => onMsg(e)).catch(() => ws.close());
  });
  ws.addEventListener('close', () => { try { remote?.close(); } catch {} });
}
