const amqp = require('amqplib');
const c = require('./config');

/**
 * Deklarasi seluruh topology. Dipanggil oleh producer & consumer
 * supaya argumen queue selalu konsisten (idempotent).
 */
async function setupTopology(ch) {
  // 1) Topic exchange utama
  await ch.assertExchange(c.EXCHANGE, 'topic', { durable: true });

  // 2) Dead Letter Exchange + Dead Letter Queue
  await ch.assertExchange(c.DLX, 'direct', { durable: true });
  await ch.assertQueue(c.DLQ, { durable: true });
  await ch.bindQueue(c.DLQ, c.DLX, c.DLQ_RK);

  // 3) Consumer A: queue success
  await ch.assertQueue(c.QUEUE_SUCCESS, { durable: true });
  await ch.bindQueue(c.QUEUE_SUCCESS, c.EXCHANGE, c.RK_SUCCESS);

  // 4) Consumer B: retry flow
  //    main queue --(nack/reject)--> retry exchange --> retry queue (TTL 5s)
  //    retry queue --(TTL habis)--> kembali ke main queue
  await ch.assertExchange(c.RETRY_EXCHANGE, 'direct', { durable: true });

  await ch.assertQueue(c.QUEUE_FAILED, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': c.RETRY_EXCHANGE,
      'x-dead-letter-routing-key': c.RETRY_RK,
    },
  });
  await ch.bindQueue(c.QUEUE_FAILED, c.EXCHANGE, c.RK_FAILED);

  await ch.assertQueue(c.RETRY_QUEUE, {
    durable: true,
    arguments: {
      'x-message-ttl': c.RETRY_TTL_MS,
      'x-dead-letter-exchange': '',                      // default exchange
      'x-dead-letter-routing-key': c.QUEUE_FAILED,       // balik ke main queue
    },
  });
  await ch.bindQueue(c.RETRY_QUEUE, c.RETRY_EXCHANGE, c.RETRY_RK);
}

module.exports = { setupTopology };

// Bisa dijalankan langsung: npm run setup
if (require.main === module) {
  (async () => {
    const conn = await amqp.connect(c.RABBITMQ_URL);
    const ch = await conn.createChannel();
    await setupTopology(ch);
    console.log('✅ Topology berhasil dibuat (exchange, queue, binding, DLX).');
    await ch.close();
    await conn.close();
  })().catch((e) => { console.error(e); process.exit(1); });
}
