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

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

let lastMaxId = 0;

// --- Inizializzazione checkpoint ---
async function initCheckpoints() {
  try {
    const [rows] = await pool.query(
      `SELECT MAX(id) AS maxId FROM rcv_log`
    );
    if (rows && rows[0]) {
      lastMaxId = rows[0].maxId || 0;
      console.log('Init checkpoints:', { lastMaxId });
    }
  } catch (err) {
    console.error('Errore initCheckpoints', err);
  }
}

// --- Fetch page con optional severity ---
// --- Fetch page con filtro opzionale severity ---
async function fetchPage(offset = 0, pageSize = PAGE_SIZE, severityFilter = null) {
  let q = `
    SELECT id, active, severity, traptime, hostname, agentip, formatline
    FROM rcv_log
  `;
  const params = [];

  // Filtro severity
  if (severityFilter !== null && !isNaN(parseInt(severityFilter))) {
    q += ' WHERE severity = ?';
    params.push(parseInt(severityFilter));
  }

  q += ' ORDER BY id DESC LIMIT ? OFFSET ?';
  params.push(pageSize, offset);

  console.log('[DEBUG] fetchPage SQL:', q, 'params:', params); // <-- debug SQL

  const [rows] = await pool.query(q, params);
  console.log('[DEBUG] fetchPage rows:', rows.map(r => r.severity));
  return rows;
}


// --- Fetch changes (new + updated) ---
// --- Fetch changes (new + updated) con filtro severity opzionale ---
async function fetchChanges(severityFilter = null) {
  // Nuove righe
  let newRowsQuery = `
    SELECT id, active, severity, traptime, hostname, agentip, formatline, updated
    FROM rcv_log
    WHERE id > ?`;
  const newParams = [lastMaxId];

  if(severityFilter !== null && !isNaN(parseInt(severityFilter))) {
    newRowsQuery += ' AND severity = ?';
    newParams.push(severityFilter);
  }

  newRowsQuery += ' ORDER BY id ASC LIMIT 5000';
  const [newRows] = await pool.query(newRowsQuery, newParams);

  // Righe aggiornate
  let updatedRowsQuery = `
    SELECT id, active, severity, traptime, hostname, agentip, formatline, updated
    FROM rcv_log
    WHERE updated = 1`;
  const updatedParams = [];

  if(severityFilter !== null && !isNaN(parseInt(severityFilter))) {
    updatedRowsQuery += ' AND severity = ?';
    updatedParams.push(severityFilter);
  }

  updatedRowsQuery += ' ORDER BY id ASC LIMIT 5000';
  const [updatedRows] = await pool.query(updatedRowsQuery, updatedParams);

  return { newRows, updatedRows };
}


// --- Invia chunk ai client WS ---
function sendChunksToAllClients(wsServer, rows, type = 'update') {
  if (!rows || rows.length === 0) return;
  let start = 0;
  while (start < rows.length) {
    const chunk = rows.slice(start, start + MAX_UPDATE_BATCH);
    const payload = JSON.stringify({ type, rows: chunk });
    wsServer.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        if (client.severityFilter === null || client.severityFilter === undefined) {
          client.send(payload);
        } else {
          const filtered = chunk.filter(r => r.severity === client.severityFilter);
          if (filtered.length > 0) {
            client.send(JSON.stringify({ type, rows: filtered }));
          }
        }
      }
    });
    start += MAX_UPDATE_BATCH;
  }
}

// --- Polling loop ---
async function pollingLoop() {
  try {
    const { newRows, updatedRows } = await fetchChanges();
    const combined = [...newRows, ...updatedRows];
    if (combined.length > 0) {
      const map = new Map();
      combined.forEach(r => map.set(String(r.id), r));
      const uniqueRows = Array.from(map.values());

      sendChunksToAllClients(wss, uniqueRows, 'update');

      const maxIdInCombined = Math.max(...uniqueRows.map(r => r.id || 0), lastMaxId);
      lastMaxId = Math.max(lastMaxId, maxIdInCombined);

      // Reset updated flag
      if(updatedRows.length > 0){
        const idsToReset = updatedRows.map(r=>r.id);
        await pool.query(`UPDATE rcv_log SET updated=0 WHERE id IN (?)`, [idsToReset]);
      }

      console.log(`[DEBUG] FetchChanges: nuove=${newRows.length}, aggiornate=${updatedRows.length}`);
      console.log(`[DEBUG] Inviate ${uniqueRows.length} righe (update) a ${wss.clients.size} client connessi`);
      console.log(`[DEBUG] Polling completato: totali unici=${uniqueRows.length}, lastMaxId=${lastMaxId}`);
    }
  } catch (err) {
    console.error('Errore nel pollingLoop:', err);
  } finally {
    setTimeout(pollingLoop, POLL_INTERVAL_MS);
  }
}

// --- WS connection ---
wss.on('connection', async function connection(ws) {
  console.log('Client connesso');
  ws.severityFilter = null;

  try {
    const rows = await fetchPage(0, PAGE_SIZE, ws.severityFilter);
    console.log(`[DEBUG] Invio iniziale ${rows.length} righe al client (severity=${ws.severityFilter})`);
    ws.send(JSON.stringify({ type: 'init', rows }));
  } catch (err) {
    console.error('Errore fetching initial page:', err);
    ws.send(JSON.stringify({ type: 'error', message: 'Errore caricamento iniziale' }));
  }

  ws.on('message', async function incoming(message) {
    try {
      console.log('[DEBUG] Messaggio ricevuto dal client:', message.toString());
      const msg = JSON.parse(message.toString());

      if(msg.type === 'getPage') {
        const offset = parseInt(msg.offset || 0, 10);
        const pageSize = parseInt(msg.pageSize || PAGE_SIZE, 10);
        const severity = (msg.severity !== undefined && msg.severity !== null) ? parseInt(msg.severity) : ws.severityFilter;
        ws.severityFilter = isNaN(severity) ? null : severity;

        console.log(`[DEBUG] getPage: offset=${offset}, pageSize=${pageSize}, severity=${ws.severityFilter}`);

        const rows = await fetchPage(offset, pageSize, ws.severityFilter);
        console.log(`[DEBUG] Invio ${rows.length} righe al client (severity=${ws.severityFilter})`);
        ws.send(JSON.stringify({ type: 'page', offset, rows }));

      } else {
        ws.send(JSON.stringify({ type: 'error', message: 'Tipo messaggio non gestito' }));
      }
    } catch(err) {
      console.error('Errore processing message from client:', err);
      ws.send(JSON.stringify({ type: 'error', message: 'Formato messaggio non valido' }));
    }
  });


  ws.on('close', () => {
    console.log('Client disconnesso');
  });
});

// --- Avvio server ---
(async () => {
  await initCheckpoints();
  server.listen(PORT, () => {
    console.log(`HTTP server + WS in ascolto su http://localhost:${PORT}`);
  });
  setTimeout(pollingLoop, POLL_INTERVAL_MS);
})();
