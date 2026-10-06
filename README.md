# RabbitMQ: 1 Producer + 2 Consumer (Topic Exchange + Retry + DLX)

Studi kasus event pembayaran memakai RabbitMQ.

## Arsitektur

```
                           ┌── binding: payment.success ──► payment.success.queue ──► Consumer A (log + update DB, manual ACK)
Producer ──► payments.event ┤
             (topic)       └── binding: payment.failed  ──► payment.failed.queue ──► Consumer B
                                                                   │  ▲                  │
                                                       nack(reject)│  │ TTL 5s habis      │ sudah retry 2x
                                                                   ▼  │                  ▼
                                                    payments.retry ──► payment.failed.retry.queue    payments.dlx ──► payments.dlq
```

| Komponen | Nama |
|---|---|
| Topic exchange | `payments.event` |
| Routing key | `payment.success`, `payment.failed` |
| Queue Consumer A | `payment.success.queue` (durable, manual ACK) |
| Queue Consumer B | `payment.failed.queue` (durable) |
| Retry exchange / queue | `payments.retry` / `payment.failed.retry.queue` (TTL 5000 ms) |
| Dead Letter Exchange / Queue | `payments.dlx` / `payments.dlq` |

Alur Consumer B: gagal -> `nack` -> masuk retry queue -> 5 detik -> kembali ke main queue. Setelah **2x retry** masih gagal, pesan dikirim ke `payments.dlx` dan masuk `payments.dlq`.

## Prasyarat
- Docker Desktop
- Node.js >= 18
- VS Code

## Cara menjalankan

```bash
# 1. Jalankan RabbitMQ
docker compose up -d

# 2. Install dependency
npm install

# 3. Buat exchange/queue/binding (opsional, otomatis juga dibuat saat app jalan)
npm run setup
```

Buka **3 terminal** di VS Code:

```bash
# Terminal 1 - Consumer A
npm run consumer:a

# Terminal 2 - Consumer B
npm run consumer:b

# Terminal 3 - Producer
npm run producer
```

Management UI: http://localhost:15672 (user `guest` / pass `guest`)

## Hasil yang diharapkan
- Pesan `payment.success` -> Consumer A mencetak log dan menyimpan ke `data/payments.json`, lalu ACK.
- Pesan `payment.failed` -> Consumer B gagal (disimulasikan), retry 2x tiap 5 detik, lalu pesan masuk `payments.dlq` (cek di UI: Queues -> `payments.dlq`).
- Agar Consumer B sukses: `FORCE_FAIL=false npm run consumer:b`

## Stop
```bash
docker compose down        # hapus container
docker compose down -v     # hapus juga data volume
```
