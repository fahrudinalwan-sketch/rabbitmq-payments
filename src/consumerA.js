const fs = require('fs');
const path = require('path');
const amqp = require('amqplib');
const c = require('./config');
const { setupTopology } = require('./setup');

// "Database" sederhana berbasis file JSON (simulasi update DB)
const DB_DIR = path.join(__dirname, '..', 'data');
const DB_FILE = path.join(DB_DIR, 'payments.json');

function updateDB(record) {
  fs.mkdirSync(DB_DIR, { recursive: true });
  const rows = fs.existsSync(DB_FILE) ? JSON.parse(fs.readFileSync(DB_FILE, 'utf8')) : [];
  rows.push({ ...record, processedAt: new Date().toISOString() });
  fs.writeFileSync(DB_FILE, JSON.stringify(rows, null, 2));
}

async function main() {
  const conn = await amqp.connect(c.RABBITMQ_URL);
  const ch = await conn.createChannel();
  await setupTopology(ch);
  await ch.prefetch(1);

  console.log(`🟢 [Consumer A] menunggu pesan di "${c.QUEUE_SUCCESS}" ...`);

  ch.consume(
    c.QUEUE_SUCCESS,
    (msg) => {
      if (!msg) return;
      try {
        const data = JSON.parse(msg.content.toString());
        console.log(`✅ [Consumer A] LOG payment success:`, data);
        updateDB(data);
        ch.ack(msg); // manual ACK setelah sukses
      } catch (err) {
        console.error('❌ [Consumer A] gagal proses:', err.message);
        ch.nack(msg, false, true); // requeue
      }
    },
    { noAck: false } // manual ACK
  );
}

main().catch((e) => { console.error(e); process.exit(1); });
