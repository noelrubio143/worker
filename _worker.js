import { connect } from 'cloudflare:sockets';

const UUID = '00000000-0000-4000-8000-000000000000';

export default {
  async fetch(request, env) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('OK', { status: 200 });
    }
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    handle(server, env.UUID || UUID, env.PROXYIP || '');
    return new Response(null, { status: 101, webSocket: client });
  }
};

function handle(ws, uuid, proxyIp) {
  let remote = null, writer = null, first = true;

  async function open(host, port, payload, header, canRetry) {
    let ended = false, got = false, sock;
    const done = () => {
      if (ended) return;
      ended = true;
      if (!got && canRetry && proxyIp && remote === sock) {
        const [h, p] = proxyIp.split(':');
        open(h, Number(p) || port, payload, header, false).catch(() => ws.close());
      } else ws.close();
    };
    try {
      sock = connect({ hostname: host, port });
      remote = sock;
      writer = sock.writable.getWriter();
      await writer.write(payload);
      sock.readable.pipeTo(new WritableStream({
        write(chunk) {
          got = true;
          if (header) {
            const m = new Uint8Array(header.length + chunk.length);
            m.set(header); m.set(chunk, header.length);
            ws.send(m); header = null;
          } else ws.send(chunk);
        },
        close: done,
        abort: done
      })).catch(done);
    } catch { done(); }
  }

  async function onMsg(e) {
    const data = new Uint8Array(
      e.data instanceof ArrayBuffer ? e.data : await e.data.arrayBuffer()
    );
    if (!first) { if (writer) await writer.write(data); return; }
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

    await open(host, port, data.slice(i), new Uint8Array([data[0], 0]), true);
  }

  let chain = Promise.resolve();
  ws.addEventListener('message', (e) => {
    chain = chain.then(() => onMsg(e)).catch(() => ws.close());
  });
  ws.addEventListener('close', () => { try { remote?.close(); } catch {} });
}
