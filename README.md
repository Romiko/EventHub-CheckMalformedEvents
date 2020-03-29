"# EventHub-CheckMalformedEvents" 

This application is designed to subscribe to events from an Azure Event Hub. It is desgined as a troubleshooting tool that will pull events from a particular partition; starting at a particular sequence number.
The subscription will end when a date in the event is reached or when the maximum time allowed is reached.


Copy appsettings-sample.json to appsettings.json
  1. Add your event hub connection string
  2. Add Consumer Group
  3. Add Partition, sequence to get the events you need to read
  4. Add ProcessingEnqueueEndTimeUTC - In Streaming Analytics Jobs, you sometimes get an error with start processing time, end processing time, this allows you to stop streaming on the end processing time.
  5. Add TerminateAfterSeconds to ensure it does not run for too long!

Example Error from Streaming Analytics that this tool can then be used to diagnose.

[17:22:00] Source 'EventHub' had 58 occurrences of kind 'InputDeserializerError.TypeConversionError' between processing times '2020-03-24T06:17:15.7425573Z' and '2020-03-24T06:22:00.5280335Z'. Could not deserialize the input event(s) from resource 'Partition: [11], Offset: [86835656249408], SequenceNumber: [137869145]' as Json. Some possible reasons: 1) Malformed events 2) Input source configured with incorrect serialization format