using System.CommandLine;
using System.CommandLine.Invocation;
using System.Threading;
using System.Threading.Channels;
using Confluent.Kafka;
using KafkaBridge.Services;

namespace KafkaBridge.Commands
{
    public class CopyCommand : Command
    {
        private readonly KafkaService _kafkaService;

        public CopyCommand(KafkaService kafkaService) 
            : base("copy", "Transfers messages from one topic to another")
        {
            _kafkaService = kafkaService;
            PrepareCommand();
            Handler = CommandHandler
                .Create<string, string, string, AutoOffsetReset, string, string, CancellationToken>(Handle);
        }

        private void PrepareCommand()
        {
            AddOption(new Option<string>(new[] {"--from-server", "-f"}, () => "localhost:9092")
            {
                Description = "Kafka source bootstrap servers"
            });

            AddOption(new Option<string>(new[] {"--src-topic", "-s"})
            {
                IsRequired = true,
                Description = "Kafka source topic"
            });

            AddOption(new Option<string>(new[] {"--group", "-g"})
            {
                IsRequired = false,
                Description = "Consumer group id"
            });

            AddOption(new Option<AutoOffsetReset>(new[] {"--auto-offset-reset", "-r"}, () => AutoOffsetReset.Latest)
            {
                IsRequired = false,
                Description = "Offset reset policy"
            });

            AddOption(new Option<string>(new[] {"--to-server", "-t"}, () => "localhost:9092")
            {
                IsRequired = false,
                Description = "Kafka destination bootstrap servers"
            });
            AddOption(new Option<string>(new[] {"--dst-topic", "-d"})
            {
                IsRequired = false,
                Description = "Kafka destination topic"
            });
        }

        private void Handle(string fromServer, string srcTopic, string group, 
            AutoOffsetReset autoOffsetReset, string toServer, string dstTopic, 
            CancellationToken cancellationToken = default)
        {
            var channel = Channel.CreateUnbounded<Message<byte[],byte[]>>();
            
            dstTopic = dstTopic.IsEmpty()
                ? srcTopic
                : dstTopic;
            
            var source = new KafkaSource(fromServer, srcTopic, group, autoOffsetReset);
            var destination = new KafkaDestination(toServer, dstTopic);
            
            var consumerTask = _kafkaService.StarConsumer(source.Topic, source.GetConsumerConfig(), channel.Writer, 
                cancellationToken);
            
            var producerTask = _kafkaService.StartProducer(destination.Topic, destination.GetProducerConfig(), channel.Reader,
                cancellationToken);
        }
    }
}