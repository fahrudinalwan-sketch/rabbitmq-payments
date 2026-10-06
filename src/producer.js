const amqp = require('amqplib');
const c = require('./config');
const { setupTopology } = require('./setup');

async function main() {
  const conn = await amqp.connect(c.RABBITMQ_URL);
  const ch = await conn.createConfirmChannel();
  await setupTopology(ch);

  const events = [
    { rk: c.RK_SUCCESS, data: { orderId: 'ORD-001', amount: 150000, status: 'SUCCESS' } },
    { rk: c.RK_FAILED,  data: { orderId: 'ORD-002', amount: 75000,  status: 'FAILED', reason: 'Saldo tidak cukup' } },
    { rk: c.RK_SUCCESS, data: { orderId: 'ORD-003', amount: 300000, status: 'SUCCESS' } },
    { rk: c.RK_FAILED,  data: { orderId: 'ORD-004', amount: 50000,  status: 'FAILED', reason: 'Kartu ditolak' } },
  ];

  for (const e of events) {
    const payload = Buffer.from(JSON.stringify({ ...e.data, createdAt: new Date().toISOString() }));
    ch.publish(c.EXCHANGE, e.rk, payload, {
      persistent: true,
      contentType: 'application/json',
    });
    console.log(`📤 [Producer] publish -> ${e.rk}`, e.data.orderId);
  }

  await ch.waitForConfirms();
  await ch.close();
  await conn.close();
  console.log('✅ Semua pesan terkirim.');
}

main().catch((e) => { console.error(e); process.exit(1); });
