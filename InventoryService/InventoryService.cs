using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Text.Json;

public class InventoryService
{
    private readonly IConnection _connection;
    private readonly IModel _channel;

    public InventoryService()
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
    }

    public void StartService()
    {
        var consumer = new EventingBasicConsumer(_channel);
        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            var data = JsonSerializer.Deserialize<SagaData>(message);

            Console.WriteLine("Inventory Service processing: " + message);

            try
            {
                // Deduct inventory
                var result = new SagaData { OrderId = data.OrderId, Status = "inventory deducted" };
                var resultMessage = JsonSerializer.Serialize(result);
                var resultBody = Encoding.UTF8.GetBytes(resultMessage);
                _channel.BasicPublish(exchange: "saga_exchange", routingKey: "inventory.done", basicProperties: null, body: resultBody);
            }
            catch (Exception ex)
            {
                var errorMessage = JsonSerializer.Serialize(new { Error = ex.Message, data.OrderId });
                var errorBody = Encoding.UTF8.GetBytes(errorMessage);
                _channel.BasicPublish(exchange: "dlx_exchange", routingKey: "inventory.dlq", basicProperties: null, body: errorBody);
            }
        };

        _channel.BasicConsume(queue: "inventory.start", autoAck: true, consumer: consumer);
    }
}