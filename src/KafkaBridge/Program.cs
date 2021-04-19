using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaBridge
{
    class Program
    {
        /// <summary>
        /// Starts kafka bridge service.
        /// </summary>
        /// <param name="fromServer">Source kafka bootstrap servers. For example, localhost:9092</param>
        /// <param name="fromTopic">Source kafka topic name</param>
        /// <param name="groupId">Consumer group id</param>
        /// <param name="autoOffsetReset">Auto offset reset</param>
        /// <param name="toServer">Destination kafka bootstrap servers. For example, localhost:9092</param>
        /// <param name="toTopic">Destination kafka topic name</param>
        /// 
        static void Main(string fromServer, string fromTopic, 
            string groupId, AutoOffsetReset autoOffsetReset, 
            string toServer, string toTopic)
        {
            var cts = new CancellationTokenSource();
            var channel = Channel.CreateUnbounded<string>();

            fromServer ??= KafkaDefaults.BootstrapServers;
            var source = new KafkaSource(fromServer, fromTopic, groupId, autoOffsetReset);
            var destination = new KafkaDestination(toServer ?? fromServer, toTopic ?? fromTopic);

            var producerTask = StartProducer(destination.Topic, destination.GetProducerConfig(), channel.Reader,
                cts.Token);
            var consumerTask = StarConsumer(source.Topic, source.GetConsumerConfig(), channel.Writer, cts.Token);

            Console.WriteLine("Press any key to stop");
            Console.ReadKey();
            cts.Cancel();
        }

        private static Task StarConsumer(string topic, ConsumerConfig config, ChannelWriter<string> writer, CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
                consumer.Subscribe(topic);

                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(cancellationToken);
                    Console.WriteLine(consumeResult.Message.Value);
                    await writer.WriteAsync(consumeResult.Message.Value, cancellationToken);
                }

                consumer.Close();
            }, cancellationToken);
        }

        private static Task StartProducer(string topic, ProducerConfig config, ChannelReader<string> reader,
            CancellationToken cancellationToken)
        {
            return Task.Run(async ()  =>
            {
                using var producer = new ProducerBuilder<Null, string>(config).Build();
                while (await reader.WaitToReadAsync(cancellationToken))
                {
                    var msg = await reader.ReadAsync(cancellationToken);
                    await producer.ProduceAsync
                    (
                        topic, new Message<Null, string>()
                        {
                            Value = msg
                        }, 
                        cancellationToken
                    );
                }
            }, cancellationToken);
        }
    }
}