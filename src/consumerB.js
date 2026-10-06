const amqp = require('amqplib');
const c = require('./config');
const { setupTopology } = require('./setup');

// Set FORCE_FAIL=false kalau mau consumer B berhasil memproses
const FORCE_FAIL = process.env.FORCE_FAIL !== 'false';

/** Hitung berapa kali pesan sudah ditolak dari main queue (dari header x-death). */
function getRetryCount(msg) {
  const xDeath = msg.properties.headers && msg.properties.headers['x-death'];
  if (!Array.isArray(xDeath)) return 0;
  const entry = xDeath.find((d) => d.queue === c.QUEUE_FAILED && d.reason === 'rejected');
  return entry ? Number(entry.count) : 0;
}

async function processFailedPayment(data) {
  // Simulasi proses (mis. panggil API refund/notifikasi) yang gagal
  if (FORCE_FAIL) throw new Error('Simulasi error saat memproses payment.failed');
  console.log('   -> berhasil diproses:', data.orderId);
}

async function main() {
  const conn = await amqp.connect(c.RABBITMQ_URL);
  const ch = await conn.createChannel();
  await setupTopology(ch);
  await ch.prefetch(1);

  console.log(`🔴 [Consumer B] menunggu pesan di "${c.QUEUE_FAILED}" (FORCE_FAIL=${FORCE_FAIL}) ...`);

  ch.consume(
    c.QUEUE_FAILED,
    async (msg) => {
      if (!msg) return;
      const data = JSON.parse(msg.content.toString());
      const retryCount = getRetryCount(msg);

      try {
        console.log(`⚙️  [Consumer B] proses ${data.orderId} (percobaan ke-${retryCount + 1})`);
        await processFailedPayment(data);
        ch.ack(msg);
      } catch (err) {
        if (retryCount >= c.MAX_RETRY) {
          // Sudah retry 2x -> kirim ke DLX/DLQ
          console.log(`💀 [Consumer B] ${data.orderId} gagal setelah ${retryCount}x retry -> DLQ (${c.DLX})`);
          ch.publish(c.DLX, c.DLQ_RK, msg.content, {
            persistent: true,
            contentType: 'application/json',
            headers: { 'x-original-error': err.message, 'x-retry-count': retryCount },
          });
          ch.ack(msg);
        } else {
          console.log(`🔁 [Consumer B] ${data.orderId} gagal (${err.message}) -> retry ${retryCount + 1}/${c.MAX_RETRY} dalam ${c.RETRY_TTL_MS / 1000}s`);
          ch.nack(msg, false, false); // requeue=false -> dead-letter ke retry exchange
        }
      }
    },
    { noAck: false }
  );
}

main().catch((e) => { console.error(e); process.exit(1); });
