using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Text.Json;
using System.Collections.Generic;

public class Orchestrator
{
    private readonly IConnection _connection;
    private readonly IModel _channel;

    public Orchestrator()
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
    }

    public void StartSaga()
    {
        _channel.ExchangeDeclare(exchange: "saga_exchange", type: "topic");

        // Declare dead letter exchange
        _channel.ExchangeDeclare(exchange: "dlx_exchange", type: "topic");

        var queues = new[]
        {
            "order.start",
            "order.done",
            "inventory.start",
            "inventory.done",
            "payment.start",
            "payment.done",
            "shipping.start",
            "shipping.done",
            "order.confirm"
        };

        foreach (var queue in queues)
        {
            _channel.QueueDeclare(queue: queue, durable: false, exclusive: false, autoDelete: false, arguments: null);
            _channel.QueueBind(queue: queue, exchange: "saga_exchange", routingKey: queue);
        }

        // Declare dead letter queues with TTL
        var dlqArgs = new Dictionary<string, object>
        {
            { "x-message-ttl", 60000 }, // TTL 60 seconds
            { "x-dead-letter-exchange", "dlx_exchange" }
        };

        var dlqQueues = new[]
        {
            "order.dlq",
            "inventory.dlq",
            "payment.dlq",
            "shipping.dlq"
        };

        foreach (var dlq in dlqQueues)
        {
            _channel.QueueDeclare(queue: dlq, durable: false, exclusive: false, autoDelete: false, arguments: dlqArgs);
            _channel.QueueBind(queue: dlq, exchange: "dlx_exchange", routingKey: dlq);
        }

        // Create consumers for done messages
        _channel.BasicConsume(queue: "order.done", autoAck: true, consumer: CreateConsumer("inventory.start"));
        _channel.BasicConsume(queue: "inventory.done", autoAck: true, consumer: CreateConsumer("payment.start"));
        _channel.BasicConsume(queue: "payment.done", autoAck: true, consumer: CreateConsumer("shipping.start"));
        _channel.BasicConsume(queue: "shipping.done", autoAck: true, consumer: CreateConsumer("order.confirm"));

        // Handle dead letter queue messages for compensation
        _channel.BasicConsume(queue: "order.dlq", autoAck: true, consumer: CreateDLQConsumer("Order Service"));
        _channel.BasicConsume(queue: "inventory.dlq", autoAck: true, consumer: CreateDLQConsumer("Inventory Service"));
        _channel.BasicConsume(queue: "payment.dlq", autoAck: true, consumer: CreateDLQConsumer("Payment Service"));
        _channel.BasicConsume(queue: "shipping.dlq", autoAck: true, consumer: CreateDLQConsumer("Shipping Service"));

        StartOrder();
    }

    private EventingBasicConsumer CreateConsumer(string nextStepRoutingKey)
    {
        var consumer = new EventingBasicConsumer(_channel);
        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            var data = JsonSerializer.Deserialize<SagaData>(message);

            Console.WriteLine($"{ea.RoutingKey} completed: " + message);

            var nextMessage = JsonSerializer.Serialize(data);
            var nextBody = Encoding.UTF8.GetBytes(nextMessage);
            _channel.BasicPublish(exchange: "saga_exchange", routingKey: nextStepRoutingKey, basicProperties: null, body: nextBody);
        };
        return consumer;
    }

    private EventingBasicConsumer CreateDLQConsumer(string serviceName)
    {
        var consumer = new EventingBasicConsumer(_channel);
        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            var data = JsonSerializer.Deserialize<SagaData>(message);

            Console.WriteLine($"{serviceName} failed: " + message);
            // Handle compensation logic here if needed
        };
        return consumer;
    }

    private void StartOrder()
    {
        var initialData = JsonSerializer.Serialize(new SagaData { OrderId = 1 });
        var initialBody = Encoding.UTF8.GetBytes(initialData);
        _channel.BasicPublish(exchange: "saga_exchange", routingKey: "order.start", basicProperties: null, body: initialBody);
    }
}