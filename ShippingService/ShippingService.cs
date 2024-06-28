using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Text.Json;

public class ShippingService
{
    private readonly IConnection _connection;
    private readonly IModel _channel;

    public ShippingService()
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

            Console.WriteLine("Shipping Service processing: " + message);

            try
            {
                // Arrange shipping
                var result = new SagaData { OrderId = data.OrderId, Status = "shipped" };
                var resultMessage = JsonSerializer.Serialize(result);
                var resultBody = Encoding.UTF8.GetBytes(resultMessage);
                _channel.BasicPublish(exchange: "saga_exchange", routingKey: "shipping.done", basicProperties: null, body: resultBody);
            }
            catch (Exception ex)
            {
                var errorMessage = JsonSerializer.Serialize(new { Error = ex.Message, data.OrderId });
                var errorBody = Encoding.UTF8.GetBytes(errorMessage);
                _channel.BasicPublish(exchange: "dlx_exchange", routingKey: "shipping.dlq", basicProperties: null, body: errorBody);
            }
        };

        _channel.BasicConsume(queue: "shipping.start", autoAck: true, consumer: consumer);
    }
}