// server.js
require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const mysql = require('mysql2/promise');

const PORT = process.env.PORT || 3000;
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '2000', 10);
const PAGE_SIZE = 50;
const MAX_UPDATE_BATCH = 250;

const pool = mysql.createPool({
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '3306', 10),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  dateStrings: true
});

const app = express();
app.use(express.static('public'));

// API per ottenere i dati della tabella mib_oid
app.get('/api/mibsobj', async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM mib_oid');
    res.json(rows);
  } catch (err) {
    console.error('Errore /api/mibsobj:', err);
    res.status(500).json({ error: 'Errore nel recupero dei dati' });
  }
});
// API per ottenere i dati della tabella nodes (escludendo id)
app.get('/api/nodes', async (req, res) => {
  try {
    const [rows] = await pool.query(`
      SELECT 
        node_name, 
        target, 
        site, 
        node_type, 
        node_model, 
        poll_interval, 
        poll_retry, 
        poll_timeout
      FROM nodes
    `);
    res.json(rows);
  } catch (err) {
    console.error('Errore /api/nodes:', err);
    res.status(500).json({ error: 'Errore nel recupero dei dati' });
  }
});
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

let lastMaxId = 0;
let lastUpdatetime = '1970-01-01 00:00:00';

async function initCheckpoints() {
  try {
    const [rows] = await pool.query(
      `SELECT MAX(id) AS maxId, MAX(updatetime) AS maxUpdatetime FROM rcv_log`
    );
    if (rows && rows[0]) {
      lastMaxId = rows[0].maxId || 0;
      lastUpdatetime = rows[0].maxUpdatetime || lastUpdatetime;
      console.log('Init checkpoints:', { lastMaxId, lastUpdatetime });
    }
  } catch (err) {
    console.error('Errore initCheckpoints', err);
  }
}

// Ottieni pagina con optional filtro severity (se severityFilter != null filtra anche active=1)
async function fetchPage(offset = 0, pageSize = PAGE_SIZE, severityFilter = null) {
  let q = `
    SELECT id, active, severity, traptime, hostname, agentip, formatline
    FROM rcv_log
  `;
  const params = [];
  if (severityFilter !== null && !isNaN(Number(severityFilter))) {
    q += ' WHERE severity = ? AND active = 1';
    params.push(severityFilter);
  }
  q += ' ORDER BY id DESC LIMIT ? OFFSET ?';
  params.push(pageSize, offset);
  const [rows] = await pool.query(q, params);
  return rows;
}

// Ottieni nuove righe e aggiornamenti
async function fetchChanges() {
  // Recupera tutte le righe nuove (id > lastMaxId)
  const newRowsQuery = `
    SELECT id, active, severity, traptime, hostname, agentip, formatline, updated
    FROM rcv_log
    WHERE id > ?
    ORDER BY id ASC
    LIMIT 5000`;
  const [newRows] = await pool.query(newRowsQuery, [lastMaxId]);

  // Recupera tutte le righe aggiornate (updated = 1)
  const updatedRowsQuery = `
    SELECT id, active, severity, traptime, hostname, agentip, formatline, updated
    FROM rcv_log
    WHERE updated = 1
    ORDER BY id ASC
    LIMIT 5000`;
  const [updatedRows] = await pool.query(updatedRowsQuery);

  if (newRows.length > 0 || updatedRows.length > 0) {
    console.log(`[DEBUG] FetchChanges: nuove=${newRows.length}, aggiornate=${updatedRows.length}`);
  }

  return { newRows, updatedRows };
}

function sendChunksToAllClients(wsServer, rows, type = 'update') {
  if (!rows || rows.length === 0) return;
  let totalSent = 0;
  let totalClients = 0;

  let start = 0;
  while (start < rows.length) {
    const chunk = rows.slice(start, start + MAX_UPDATE_BATCH);
    const payload = JSON.stringify({ type, rows: chunk });
    wsServer.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        totalClients++;
        // Se il client ha un filtro attivo, invia solo le righe che corrispondono e che sono active=1
        if (client.severityFilter === null || client.severityFilter === undefined) {
          client.send(payload);
          totalSent += chunk.length;
        } else {
          const filtered = chunk.filter(r => Number(r.severity) === Number(client.severityFilter) && Number(r.active) === 1);
          if (filtered.length > 0) {
            client.send(JSON.stringify({ type, rows: filtered }));
            totalSent += filtered.length;
          }
        }
      }
    });
    start += MAX_UPDATE_BATCH;
  }

  console.log(`[DEBUG] Inviate ${totalSent} righe (${type}) a ${totalClients} client connessi`);
}

async function pollingLoop() {
  try {
    const { newRows, updatedRows } = await fetchChanges();
    const combined = [...newRows, ...updatedRows];

    if (combined.length > 0) {
      const map = new Map();
      combined.forEach(r => map.set(String(r.id), r));
      const uniqueRows = Array.from(map.values());

      // Invia ai client
      sendChunksToAllClients(wss, uniqueRows, 'update');

      // Aggiorna lastMaxId
      const maxIdInCombined = Math.max(...uniqueRows.map(r => r.id || 0), lastMaxId);
      lastMaxId = Math.max(lastMaxId, maxIdInCombined);

      console.log(`[DEBUG] Polling completato: totali unici=${uniqueRows.length}, lastMaxId=${lastMaxId}`);

      // Reset del flag updated sulle righe inviate
      const idsToReset = uniqueRows.map(r => r.id);
      if (idsToReset.length > 0) {
        const placeholders = idsToReset.map(() => '?').join(',');
        await pool.query(`UPDATE rcv_log SET updated = 0 WHERE id IN (${placeholders})`, idsToReset);
      }
    }
  } catch (err) {
    console.error('Errore nel pollingLoop:', err);
  } finally {
    setTimeout(pollingLoop, POLL_INTERVAL_MS);
  }
}

wss.on('connection', async function connection(ws, req) {
  console.log('Client connesso');

  // salva filtro severity sul client, default null (nessun filtro)
  ws.severityFilter = null;

  try {
    const rows = await fetchPage(0, PAGE_SIZE, ws.severityFilter);
    ws.send(JSON.stringify({ type: 'init', rows }));
  } catch (err) {
    console.error('Errore fetching initial page:', err);
    ws.send(JSON.stringify({ type: 'error', message: 'Errore caricamento iniziale' }));
  }

  ws.on('message', async function incoming(message) {
    try {
      const msg = JSON.parse(message.toString());

      if (msg.type === 'getPage') {
        const offset = parseInt(msg.offset || 0, 10);
        const pageSize = parseInt(msg.pageSize || PAGE_SIZE, 10);

        // ===== nuova logica robusta per interpretare severity
        if ('severity' in msg) {
          if (msg.severity === null) {
            ws.severityFilter = null; // reset esplicito
          } else {
            const s = parseInt(msg.severity, 10);
            ws.severityFilter = isNaN(s) ? null : s;
          }
        }
        // ===== fine parsing

        const rows = await fetchPage(offset, pageSize, ws.severityFilter);
        ws.send(JSON.stringify({ type: 'page', offset, rows }));

        console.log(`[DEBUG] Client ha richiesto pagina offset=${offset}, pageSize=${pageSize}, filtro=${ws.severityFilter ?? 'nessuno'} -> ${rows.length} righe`);
      } else {
        ws.send(JSON.stringify({ type: 'error', message: 'Tipo messaggio non gestito' }));
      }
    } catch (err) {
      console.error('Errore processing message from client:', err);
      ws.send(JSON.stringify({ type: 'error', message: 'Formato messaggio non valido' }));
    }
  });

  ws.on('close', () => {
    console.log('Client disconnesso');
  });
});

(async () => {
  await initCheckpoints();
  server.listen(PORT, () => {
    console.log(`HTTP server + WS in ascolto su http://localhost:${PORT}`);
  });
  setTimeout(pollingLoop, POLL_INTERVAL_MS);
})();
