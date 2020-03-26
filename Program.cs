 using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs.Consumer;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

namespace CheckMalformedEvents
{
    class GetMalformedEvents
    {
        private static string partitionId;

        static void Main(string[] args)

        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();

            var connectionString = configuration.GetConnectionString("eventhub");
            var consumergroup = configuration.GetConnectionString("consumergroup");
            var eventHubClient = new EventHubConsumerClient(consumergroup, connectionString);

            Console.WriteLine("This tool is used to troubleshoot malformed messages in an Azure EventHub");
            Console.WriteLine("Sample Error Message to troubleshoot - First get the errors from the Streaming Analytics Jobs Input blade.\r\n");

            partitionId = configuration["partitionId"];
            long offset;
            if (long.TryParse(configuration["offsetNumber"], out offset) == false)
            {
                Console.Write("Enter a valid offset value.");
                return;
            }

            long sequenceNumber;
            if (long.TryParse(configuration["SequenceNumber"], out sequenceNumber) == false)
            {
                Console.Write("Enter a valid SequenceNumber value.");
                return;
            }

            var processingEnqueueEndTimeUTC = DateTimeOffset.Parse(configuration["ProcessingEnqueueEndTimeUTC"]);

            EventPosition startingPosition = EventPosition.FromSequenceNumber(sequenceNumber);
            try
            {
                GetEvents(eventHubClient, startingPosition, processingEnqueueEndTimeUTC).Wait();
            }
            catch (AggregateException e)
            {
                Console.WriteLine($"{e.Message}");

            }
        }

        private static async Task<CancellationTokenSource> GetEvents(EventHubConsumerClient eventHubClient, EventPosition startingPosition, DateTimeOffset endEnqueueTime)
        {
            var cancellationSource = new CancellationTokenSource();
            cancellationSource.CancelAfter(TimeSpan.FromMinutes(5));
            string path = Path.Combine(Directory.GetCurrentDirectory(), "Events.json");
            using var sw = new StreamWriter(path);

            var dataList = new List<dynamic>();
            var jsettings = new JsonSerializerSettings
            {
                Formatting = Formatting.Indented
            };

            int count = 0;
            await foreach (PartitionEvent receivedEvent in eventHubClient.ReadEventsFromPartitionAsync(partitionId, startingPosition, cancellationSource.Token))
            {
                using var sr = new StreamReader(receivedEvent.Data.BodyAsStream);
                var data = sr.ReadToEnd();
                var partition = receivedEvent.Data.PartitionKey;
                var offset = receivedEvent.Data.Offset;
                var sequence = receivedEvent.Data.SequenceNumber;

                try
                {
                    dynamic message = JsonConvert.DeserializeObject(data);
                    message.AzureEventHubsPartition = partition;
                    message.AzureEventHubsOffset = offset;
                    message.AzureEventHubsSequence = sequence;
                    message.AzureEnqueuedTime = receivedEvent.Data.EnqueuedTime.ToString("o");
                    dataList.Add(message);

                    if (count == 0)
                        Console.WriteLine($"First Message EnqueueTime: {message.AzureEnqueuedTime}, Offset: {message.AzureEventHubsOffset}, Sequence: {message.AzureEventHubsSequence}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Serialization issue Partition: { partition}, Offset: {offset}, Sequence Number: { sequence }");
                    Console.WriteLine(ex.Message);
                }

                if (receivedEvent.Data.EnqueuedTime > endEnqueueTime)
                {
                    Console.WriteLine($"Last Message EnqueueTime: {receivedEvent.Data.EnqueuedTime:o}, Offset: {receivedEvent.Data.Offset}, Sequence: {receivedEvent.Data.SequenceNumber}");
                    Console.WriteLine($"Total Events Streamed: {dataList.Count}");
                    Console.WriteLine($"-----------");
                    break;
                }
                count++;
            }

            var output = JsonConvert.SerializeObject(dataList, jsettings);
            sw.WriteLine(output);
            Console.WriteLine($"\r\n Output located at: {path}");
            return cancellationSource;
        }
    }
}