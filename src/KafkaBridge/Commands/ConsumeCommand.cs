using System.CommandLine;
using System.CommandLine.Invocation;
using System.Threading;
using System.Threading.Channels;
using Confluent.Kafka;
using KafkaBridge.Services;

namespace KafkaBridge.Commands
{
    public class ConsumeCommand : Command
    {
        private readonly KafkaService _kafkaService;

        public ConsumeCommand(KafkaService kafkaService)
            : base("consume", "Consumes messages from a topic")
        {
            _kafkaService = kafkaService;
            PrepareCommand();
            Handler = CommandHandler
                .Create<string, string, string, AutoOffsetReset, CancellationToken>(Handle);
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

            AddOption(new Option<AutoOffsetReset>(new[] {"--auto-offset-reset", "-r"}, 
                () => AutoOffsetReset.Latest)
            {
                IsRequired = false,
                Description = "Offset reset policy"
            });
        }

        private void Handle(string fromServer, string srcTopic, string group,
            AutoOffsetReset autoOffsetReset, CancellationToken cancellationToken)
        {
            var channel = Channel.CreateUnbounded<Message<byte[],byte[]>>();

            var source = new KafkaSource(fromServer, srcTopic, group, autoOffsetReset);

            var consumerTask = _kafkaService.StarConsumer(source.Topic, source.GetConsumerConfig(), channel.Writer, 
                cancellationToken);

            var printTask = _kafkaService.StartPrintMessages(channel.Reader, cancellationToken);
        }
    }
}