using Confluent.Kafka;
using Dawn;

namespace KafkaBridge
{
    public record KafkaDestination
    {
        public KafkaDestination(string bootstrapServers, string topic)
        {
            BootstrapServers = Guard.Argument(bootstrapServers, nameof(bootstrapServers))
                .NotNull("BootstrapServers must be set")
                .NotEmpty("BootstrapServers must be set");
            
            Topic = Guard.Argument(topic, nameof(topic))
                .NotNull("Topic must be set")
                .NotEmpty("Topic must be set");
        }
        
        public string BootstrapServers { get; }

        public string Topic { get; }
        
        public ProducerConfig GetProducerConfig()
        {
            var cfg = new ProducerConfig();
            cfg.BootstrapServers = BootstrapServers;

            return cfg;
        }
    }
}