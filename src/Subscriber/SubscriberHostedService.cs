using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Messages;
using Messages.Serialization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Subscriber
{
    public class SubscriberHostedService : BackgroundService
    {
        private readonly ILogger<SubscriberHostedService> _logger;
        private readonly IConsumer<Ignore, Hello> _consumer;

        public SubscriberHostedService(ILogger<SubscriberHostedService> logger)
        {
            _logger = logger;

            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                AutoCommitIntervalMs = 0
            };

            _consumer = new ConsumerBuilder<Ignore, Hello>(conf)
                .SetValueDeserializer(JsonMessageDeserializer<Hello>.Instance)
                .Build();
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
            => Task.Factory.StartNew(() =>
                {
                    _logger.LogInformation("Subscribing");
                    _consumer.Subscribe("test-topic");

                    while (!stoppingToken.IsCancellationRequested)
                    {
                        _logger.LogInformation("Waiting for message...");
                        
                        var message = _consumer.Consume(stoppingToken);
                        
                        _logger.LogInformation(
                            "Consumed message '{@message}' at: '{topicOffset}'.",
                            JsonSerializer.Serialize(message.Value),
                            message.TopicPartitionOffset);
                        
                        _consumer.Commit(); // note: committing every time can have a negative impact on performance
                    }
                },
                TaskCreationOptions.LongRunning);


        public override void Dispose()
        {
            _consumer.Close();
            _consumer.Dispose();
            base.Dispose();
        }
    }
}
