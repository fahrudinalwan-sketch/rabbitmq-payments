module.exports = {
  RABBITMQ_URL: process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672',

  // Exchange utama (topic)
  EXCHANGE: 'payments.event',
  RK_SUCCESS: 'payment.success',
  RK_FAILED: 'payment.failed',

  // Consumer A
  QUEUE_SUCCESS: 'payment.success.queue',

  // Consumer B
  QUEUE_FAILED: 'payment.failed.queue',
  RETRY_EXCHANGE: 'payments.retry',
  RETRY_QUEUE: 'payment.failed.retry.queue',
  RETRY_RK: 'payment.failed.retry',
  RETRY_TTL_MS: 5000,   // TTL 5 detik
  MAX_RETRY: 2,         // retry 2x

  // Dead Letter
  DLX: 'payments.dlx',
  DLQ: 'payments.dlq',
  DLQ_RK: 'payment.failed.dead',
};
