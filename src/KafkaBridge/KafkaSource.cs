using System;
using Confluent.Kafka;
using Dawn;

namespace KafkaBridge
{
    public record KafkaSource
    {
        public KafkaSource(string bootstrapServers, string topic, string groupId, AutoOffsetReset autoOffsetReset)
        {
            BootstrapServers = Guard.Argument(bootstrapServers, nameof(bootstrapServers))
                .NotNull("BootstrapServers must be set")
                .NotEmpty("BootstrapServers must be set");
            
            Topic = Guard.Argument(topic, nameof(topic))
                .NotNull("Topic must be set")
                .NotEmpty("Topic must be set");

            GroupId = groupId;
            AutoOffsetReset = autoOffsetReset;
        }
        
        public string BootstrapServers { get; }

        public string Topic { get; }

        public string GroupId { get; }
        public AutoOffsetReset AutoOffsetReset { get; }

        public ConsumerConfig GetConsumerConfig()
        {
            var cfg = new ConsumerConfig();
            cfg.BootstrapServers = BootstrapServers;
            cfg.GroupId = GroupId ?? $"kafka-bridge {Guid.NewGuid()}";
            cfg.AutoOffsetReset = AutoOffsetReset;

            return cfg;
        }
    }
}