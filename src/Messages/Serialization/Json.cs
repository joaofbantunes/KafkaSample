using System;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace Messages.Serialization
{
    public class JsonMessageSerializer<T> : ISerializer<T> where T : class
    {
        private JsonMessageSerializer()
        {
        }

        public static JsonMessageSerializer<T> Instance { get; } = new JsonMessageSerializer<T>();
        
        public byte[] Serialize(T data, SerializationContext context)
            => Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
    }

    public class JsonMessageDeserializer<T> : IDeserializer<T> where T : class
    {
        private JsonMessageDeserializer()
        {
        }

        public static JsonMessageDeserializer<T> Instance { get; } = new JsonMessageDeserializer<T>();
        
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            => isNull
                ? null
                : JsonSerializer.Deserialize<T>(data);
    }
}