1. Как собрать проект:
mvn clean package

2. Как запустить Kafka через Docker:
docker compose up -d

3. Как создать топик:
# создание темы (topic) в Kafka со следующими параметрами (количесвто партиций в теме - 3; количество реплик в каждой партиции - 2)
docker exec -it kafka1 bash
kafka-topics --create --topic my-topic --partitions 3 --replication-factor 2 --if-not-exists --bootstrap-server kafka1:9092,kafka2:9092,kafka3:9092
# проеврка создания топика
kafka-topics --list --bootstrap-server kafka1:9092,kafka2:9092,kafka3:9092
# подробная информация о топике
kafka-topics --bootstrap-server kafka1:9092 --describe --topic my-topic
# после успешного выполнения проверки создания топика
exit

4. Как запустить продюсера и консумеров:
# Запуск продюсера и консумеров
java -cp target/kafka-practice-1-1.0-SNAPSHOT.jar com.github.maximslepukhin.producer.ProducerApp &
java -cp target/kafka-practice-1-1.0-SNAPSHOT.jar com.github.maximslepukhin.singleConsumer.SingleMessageConsumer &
java -cp target/kafka-practice-1-1.0-SNAPSHOT.jar com.github.maximslepukhin.batchConsumer.BatchMessageConsumer &
wait
