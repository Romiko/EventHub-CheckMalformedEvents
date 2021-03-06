﻿using System;
using System.IO;
using System.Text;
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
        private static IConfigurationRoot configuration;
        private static string connectionString;
        private static string consumergroup;
        private static EventHubConsumerClient eventHubClient;
        private static EventPosition startingPosition;
        private static DateTimeOffset processingEnqueueEndTimeUTC;

        static void Main(string[] args)

        {
            Init();
            ShowIntro();

            try
            {
                GetEvents(eventHubClient, startingPosition, processingEnqueueEndTimeUTC).Wait();
            }
            catch (AggregateException e)
            {
                Console.WriteLine($"{e.Message}");

            }
            catch (Exception e)
            {
                Console.WriteLine($"{e.Message}");
            }
        }

        private static void Init()
        {
            var builder = new ConfigurationBuilder()
                            .SetBasePath(Directory.GetCurrentDirectory())
                            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            configuration = builder.Build();

            connectionString = configuration.GetConnectionString("eventhub");
            consumergroup = configuration.GetConnectionString("consumergroup");
            eventHubClient = new EventHubConsumerClient(consumergroup, connectionString);

            partitionId = configuration["partitionId"];
            if (long.TryParse(configuration["SequenceNumber"], out long sequenceNumber) == false)
                throw new ArgumentException("Invalid SequenceNumber");

            processingEnqueueEndTimeUTC = DateTimeOffset.Parse(configuration["ProcessingEnqueueEndTimeUTC"]);
            startingPosition = EventPosition.FromSequenceNumber(sequenceNumber);
        }

        private static void ShowIntro()
        {
            Console.WriteLine("This tool is used to troubleshoot malformed messages in an Azure EventHub");
            Console.WriteLine("Sample Error Message to troubleshoot - First get the errors from the Streaming Analytics Jobs Input blade.\r\n");
        }

        private static async Task<CancellationTokenSource> GetEvents(EventHubConsumerClient eventHubClient, EventPosition startingPosition, DateTimeOffset endEnqueueTime)
        {
            var cancellationSource = new CancellationTokenSource();
            if (int.TryParse(configuration["TerminateAfterSeconds"], out int TerminateAfterSeconds) == false)
                throw new ArgumentException("Invalid TerminateAfterSeconds");

            cancellationSource.CancelAfter(TimeSpan.FromSeconds(TerminateAfterSeconds));
            string path = Path.Combine(Directory.GetCurrentDirectory(), $"output-{Path.GetRandomFileName()}.json");

            int count = 0;
            byte[] encodedText;
            using FileStream sourceStream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Write, bufferSize: 4096, useAsync: true);
            {
                encodedText = Encoding.Unicode.GetBytes("{\r\n\"events\": [" + Environment.NewLine);
                await sourceStream.WriteAsync(encodedText, 0, encodedText.Length);
                encodedText = Encoding.Unicode.GetBytes("");
                await foreach (PartitionEvent receivedEvent in eventHubClient.ReadEventsFromPartitionAsync(partitionId, startingPosition, cancellationSource.Token))
                {
                    if (encodedText.Length > 0)
                        await sourceStream.WriteAsync(encodedText, 0, encodedText.Length);

                    count++;
                    using var sr = new StreamReader(receivedEvent.Data.BodyAsStream);
                    var data = sr.ReadToEnd();
                    var partition = receivedEvent.Data.PartitionKey;
                    var offset = receivedEvent.Data.Offset;
                    var sequence = receivedEvent.Data.SequenceNumber;

                    try
                    {
                        dynamic message = AddMetaData(count, receivedEvent, data, partition, offset, sequence);
                        var textWithMetaData = JsonConvert.SerializeObject(message);
                        encodedText = Encoding.Unicode.GetBytes(textWithMetaData + "," + Environment.NewLine);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Serialization issue Partition: { partition}, Offset: {offset}, Sequence Number: { sequence }");
                        Console.WriteLine(ex.Message);
                    }

                    if (receivedEvent.Data.EnqueuedTime > endEnqueueTime)
                    {
                        Console.WriteLine($"Last Message EnqueueTime: {receivedEvent.Data.EnqueuedTime:o}, Offset: {receivedEvent.Data.Offset}, Sequence: {receivedEvent.Data.SequenceNumber}");
                        Console.WriteLine($"Total Events Streamed: {count}");
                        Console.WriteLine($"-----------");
                        break;
                    }
                }
                encodedText = await FinaliseFile(encodedText, sourceStream);
            }

            Console.WriteLine($"\r\n Output located at: {path}");
            return cancellationSource;
        }

        private static async Task<byte[]> FinaliseFile(byte[] encodedText, FileStream sourceStream)
        {
            await sourceStream.WriteAsync(encodedText, 0, encodedText.Length - 6); //Remove ,\r\n on last line
            encodedText = Encoding.Unicode.GetBytes("]\r\n}" + Environment.NewLine);
            await sourceStream.WriteAsync(encodedText, 0, encodedText.Length);
            return encodedText;
        }

        private static dynamic AddMetaData(int count, PartitionEvent receivedEvent, string data, string partition, long offset, long sequence)
        {
            dynamic message = JsonConvert.DeserializeObject(data);
            message.AzureEventHubsPartition = partition;
            message.AzureEventHubsOffset = offset;
            message.AzureEventHubsSequence = sequence;
            message.AzureEnqueuedTime = receivedEvent.Data.EnqueuedTime.ToString("o");

            if (count == 0)
                Console.WriteLine($"First Message EnqueueTime: {message.AzureEnqueuedTime}, Offset: {message.AzureEventHubsOffset}, Sequence: {message.AzureEventHubsSequence}");

            return message;
        }
    }
}
