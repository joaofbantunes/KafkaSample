using System;
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
        {
            _logger.LogInformation("Subscribing");
            _consumer.Subscribe("test-topic");
            
            var tcs = new TaskCompletionSource<bool>();

            // polling for messages is a blocking operation,
            // so spawning a new thread to keep doing it in the background
            var thread = new Thread(() =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        _logger.LogInformation("Waiting for message...");

                        var message = _consumer.Consume(stoppingToken);

                        _logger.LogInformation(
                            "Consumed message '{@message}' at: '{topicOffset}'.",
                            JsonSerializer.Serialize(message.Value),
                            message.TopicPartitionOffset);

                        _consumer.Commit(); // note: committing every time can have a negative impact on performance
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        _logger.LogInformation("Shutting down gracefully.");
                    }
                    catch (Exception ex)
                    {
                        // TODO: implement error handling/retry logic
                        // like this, the failed message will eventually be "marked as processed"
                        // (commit to a newer offset) even though it failed
                        _logger.LogError(ex, "Error occurred when consuming message!");
                    }
                }

                tcs.SetResult(true);
            })
            {
                IsBackground = true
            };

            thread.Start();

            return tcs.Task;
        }

        public override void Dispose()
        {
            try
            {
                _consumer?.Close();
            }
            catch (Exception)
            {
                // no exceptions in Dispose :)
            }

            _consumer?.Dispose();
            base.Dispose();
        }
    }
}