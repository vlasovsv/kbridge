using System;
using Confluent.Kafka;
using Dawn;

namespace KafkaBridge
{
    public record KafkaSource
    {
        public KafkaSource(string bootstrapServers, string topic)
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

        public ConsumerConfig GetConsumerConfig()
        {
            var cfg = new ConsumerConfig();
            cfg.BootstrapServers = BootstrapServers;
            cfg.GroupId = $"kafka-bridge {Guid.NewGuid()}";
            cfg.AutoOffsetReset = AutoOffsetReset.Earliest;

            return cfg;
        }
    }
}