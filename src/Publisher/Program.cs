using System;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Messages;
using Messages.Serialization;

namespace Publisher
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
            };

            using var producer = new ProducerBuilder<Null, Hello>(config)
                    .SetValueSerializer(JsonMessageSerializer<Hello>.Instance)
                    .Build();

            for (var i = 0; i < 50; ++i)
            {
                try
                {
                    var deliveryResult = await producer.ProduceAsync(
                        "test-topic",
                        new Message<Null, Hello>
                        {
                            Value = new Hello {Greeting = "Hi", Name = i.ToString()}
                        });
                    
                    Console.WriteLine($"Delivered '{JsonSerializer.Serialize(deliveryResult.Value)}' to '{deliveryResult.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, Hello> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }
}