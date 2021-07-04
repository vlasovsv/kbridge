using System;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using Confluent.Kafka;

namespace KafkaBridge.Services
{
    public class KafkaService
    {
        public Task StarConsumer(string topic, ConsumerConfig config, 
            ChannelWriter<Message<byte[],byte[]>> writer, 
            CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                using var consumer = new ConsumerBuilder<byte[], byte[]>(config).Build();
                consumer.Subscribe(topic);

                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(cancellationToken);
                    await writer.WriteAsync(consumeResult.Message, cancellationToken);
                }

                consumer.Close();
            }, cancellationToken);
        }

        public Task StartProducer(string topic, ProducerConfig config, 
            ChannelReader<Message<byte[],byte[]>> reader,
            CancellationToken cancellationToken)
        {
            return Task.Run(async ()  =>
            {
                using var producer = new ProducerBuilder<byte[], byte[]>(config).Build();
                while (await reader.WaitToReadAsync(cancellationToken))
                {
                    var msg = await reader.ReadAsync(cancellationToken);
                    await producer.ProduceAsync
                    (
                        topic, new Message<byte[], byte[]>()
                        {
                            Key = msg.Key,
                            Value = msg.Value
                        }, 
                        cancellationToken
                    );
                    
                    PrintMessage(msg);
                }
            }, cancellationToken);
        }

        public Task StartPrintMessages(ChannelReader<Message<byte[], byte[]>> reader,
            CancellationToken cancellationToken)
        {
            return Task.Run(async ()  =>
            {
                while (await reader.WaitToReadAsync(cancellationToken))
                {
                    var msg = await reader.ReadAsync(cancellationToken);
                    PrintMessage(msg);
                }
            }, cancellationToken);
        }

        private void PrintMessage(Message<byte[], byte[]> message)
        {
            var msgContent = Encoding.UTF8.GetString(message.Value);
            Console.WriteLine(msgContent);
        }
    }
}