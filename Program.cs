using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Threading;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace MsgConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            ThreadPool.QueueUserWorkItem(async _ => 
            {
                var healthCheckListener = new HttpListener();
                healthCheckListener.Prefixes.Add("http://*/");
                healthCheckListener.Start();
                while(true)
                {
                    var ctx = await healthCheckListener.GetContextAsync();
                    ctx.Response.StatusCode = 200;
                    ctx.Response.OutputStream.Close();
                }
            });

            var queueUrl = Environment.GetEnvironmentVariable("QUEUE_URL");
            var client = new AmazonSQSClient();
            while (true)
            {
                var receiveRequest = new ReceiveMessageRequest(queueUrl){
                    WaitTimeSeconds = 20,
                    AttributeNames = new List<string> { "All" },
                    MessageAttributeNames = new List<string> { "All" }
                };
                var response = client.ReceiveMessageAsync(receiveRequest).Result;
                var received = DateTime.UtcNow;
                Console.WriteLine($"Received {response.Messages.Count} messages");
                foreach (var message in response.Messages)
                {
                    Console.WriteLine(message.MessageAttributes.Count);
                    var sentAttr = message.MessageAttributes["Sent"];
                    var sent = DateTime.Parse(sentAttr.StringValue, null, DateTimeStyles.RoundtripKind);

                    var sysSentAttr = message.Attributes["SentTimestamp"];
                    var sysSent = DateTime.UnixEpoch.AddMilliseconds(double.Parse(sysSentAttr));

                    Console.WriteLine($"Received message: {message.Body} (sent={sent:o}, sysSent={sysSent:o}, queueTime={received-sent})");
                    client.DeleteMessageAsync(queueUrl, message.ReceiptHandle).Wait();
                }
            }
        }
    }
}
