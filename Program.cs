using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs.Consumer;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

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
            Console.WriteLine("[11:36:35] Source 'EventHub' had 76 occurrences of kind 'InputDeserializerError.TypeConversionError' between processing times '2020-03-24T00:31:36.1109029Z' and '2020-03-24T00:36:35.9676583Z'. Could not deserialize the input event(s) from resource 'Partition: [11], Offset: [86672449297304], SequenceNumber: [137530194]' as Json. Some possible reasons: 1) Malformed events 2) Input source configured with incorrect serialization format\r\n");

            partitionId = configuration["partitionId"];
            long offset;
            if (long.TryParse(configuration["offsetNumber"], out offset) == false)
            {
                Console.Write("Enter a valid offset value.");
                Console.ReadLine();
                return;
            }

            long sequenceNumber;
            if (long.TryParse(configuration["SequenceNumber"], out sequenceNumber) == false)
            {
                Console.Write("Enter a valid SequenceNumber value.");
                Console.ReadLine();
                return;
            }

            int streamDataInSeconds = int.Parse(configuration["StreamDataInSeconds"]);

            EventPosition startingPosition = EventPosition.FromSequenceNumber(sequenceNumber);
            try
            {
                GetEvents(eventHubClient, startingPosition, streamDataInSeconds).Wait();
            }
            catch (AggregateException e)
            {
                Console.WriteLine($"{e.Message}");

            }
        }

        private static async Task<CancellationTokenSource> GetEvents(EventHubConsumerClient eventHubClient, EventPosition startingPosition, int streamDataInSeconds)
        {
            var cancellationSource = new CancellationTokenSource();
            cancellationSource.CancelAfter(TimeSpan.FromSeconds(streamDataInSeconds));
            string path = Path.Combine(Directory.GetCurrentDirectory(), "Events.json");
            using var sw = new StreamWriter(path);

            await foreach (PartitionEvent receivedEvent in eventHubClient.ReadEventsFromPartitionAsync(partitionId, startingPosition, cancellationSource.Token))
            {
                using var sr = new StreamReader(receivedEvent.Data.BodyAsStream);
                var data = sr.ReadToEnd();
                var sequence = receivedEvent.Data.SequenceNumber;
                sw.WriteLine($"Sequence Number: { sequence } \r\n {data}");

                var converter = new ExpandoObjectConverter();
                dynamic message = JsonConvert.DeserializeObject<ExpandoObjectConverter>(data);
                Console.WriteLine($"Json is valid for sequence {sequence}");
            }

            Console.WriteLine($"\r\n Output located at: {path}");
            return cancellationSource;
        }
    }
}