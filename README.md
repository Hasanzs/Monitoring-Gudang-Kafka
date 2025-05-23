# 📦 Real-Time Warehouse Monitoring with Kafka & PySpark

Sebuah sistem pemantauan gudang secara real-time untuk data **suhu** dan **kelembaban**, menggunakan **Apache Kafka** sebagai messaging system dan **PySpark Structured Streaming** untuk pengolahan data secara live.

---

## 🧠 Deskripsi Proyek

Proyek ini mensimulasikan dan memproses data sensor suhu dan kelembaban dari beberapa gudang (misalnya: G1, G2, G3) dengan alur sebagai berikut:

1. **Producer Kafka** mengirimkan data sensor suhu dan kelembaban tiap detik.
2. **PySpark Streaming Consumer** membaca kedua aliran data.
3. Sistem memberikan:
   - ⚠️ *Peringatan suhu tinggi* (suhu > 80°C)
   - ⚠️ *Peringatan kelembaban tinggi* (kelembaban > 70%)
   - 🔥 *PERINGATAN KRITIS* jika keduanya tinggi dalam jendela waktu 10 detik di gudang yang sama.

---

## 📦 Struktur Topik Kafka

| Topik                      | Deskripsi                        |
|---------------------------|----------------------------------|
| `sensor-suhu-gudang`      | Data suhu dari masing-masing gudang |
| `sensor-kelembaban-gudang`| Data kelembaban dari gudang      |

---

## 📤 Format Data Producer

### Suhu
```json
{"gudang_id": "G1", "suhu": 82}
```

### Kelembaban 
```json
{"gudang_id": "G1", "kelembaban": 75}
```

## Test Simulasi

1. # Jalankan Zookeeper
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
![Zookeeper1](https://github.com/user-attachments/assets/d170efc9-c27a-4607-8df8-a047064a0ac4)

2. # Jalankan Kafka Broker
.\bin\windows\kafka-server-start.bat .\config\server.properties
![kafka server 2](https://github.com/user-attachments/assets/3c06c1e4-2e8a-41e7-bbed-784106e44497)

3. # Buat Topic Kafka 
.\bin\windows\kafka-topics.bat --create --topic sensor-suhu-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

.\bin\windows\kafka-topics.bat --create --topic sensor-kelembaban-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
![buat topic](https://github.com/user-attachments/assets/28fc3b5e-a8da-4a3e-9d67-82a0ebbf3c05)


4. # Jalankan Simulasi Producer (Python)
python producer_suhu.py
![image](https://github.com/user-attachments/assets/a1bcaab1-6091-42ab-a8dd-b42477684da5)

python producer_kelembaban.py
![image](https://github.com/user-attachments/assets/d92fd240-0fed-48e1-9b77-2192777bbde0)

5. # Jalankan PySpark Streaming
   spark-submit spark_consumer.py
![image](https://github.com/user-attachments/assets/bb779cb7-7996-463b-95d9-c0f093e3149c)






