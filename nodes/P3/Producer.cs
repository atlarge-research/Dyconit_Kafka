using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Diagnostics;
using Serilog;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;

class Producer
{
    private static int _id;
    private static string _topic;
    private static int _runtime;
    private static bool _running = true;
    private static int msgCount = 0;
    private static int tmpMsgCount = 0;
    private static double delay = 0;
    private static int _MaxDelay;
    private static int _MinDelay;
    private static int _speedupStepSize;
    private static int _throttleMinDelay = 0;
    private static double _throttleTime = 0;
    private static Random _random = new Random();
    static async Task Main(string[] args)
    {
        ParseConfig();
        ConfigureLogging();
        CancellationTokenSource ctsToken = new CancellationTokenSource();
        await CreateTopicAsync("p" + _id + "_producer_update");
        var producerTasks = new List<Task>
        {
            Task.Run(() => SendMessages(600, ctsToken)),
            Task.Run(() => ReportPerformance(5000)),
            Task.Run(() => DetermineDelay(1000)),
            Task.Run(() => ConsumeUpdates(ctsToken))
        };

        await Task.WhenAll(producerTasks);
    }
    static async Task SendMessages(int numOfMsgs, CancellationTokenSource ctsToken)
    {
        var config = new ProducerConfig { BootstrapServers = "broker:9092" };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            var timeStamp = Stopwatch.GetTimestamp();
            var tempTimeStamp = Stopwatch.GetTimestamp();
            TimeSpan timeElapsed;
            while (Stopwatch.GetElapsedTime(timeStamp).TotalMinutes < _runtime)
            {
                timeElapsed = Stopwatch.GetElapsedTime(tempTimeStamp);

                _throttleTime -= Math.Max(timeElapsed.TotalMilliseconds, 0);
                if (_throttleTime == 0)
                    _throttleMinDelay = 0;

                var diff = _throttleMinDelay - timeElapsed.TotalMilliseconds;
                if (diff > 0)
                {
                    await Task.Delay((int)diff);
                }
                tempTimeStamp = Stopwatch.GetTimestamp();

                Log.Information("Sending message " + msgCount);
                string message = "a";
                _ = await producer.ProduceAsync(_topic, new Message<Null, string> { Value = message });
                await Task.Delay((int)delay); // Delay to control the message rate

                msgCount++;
                tmpMsgCount++;
            }
            ctsToken.Cancel();
            _running = false;
        }
    }
    static async Task DetermineDelay(int updateRate)
    {
        delay = _MaxDelay;
        while (_running)
        {
            delay = _random.Next(_MinDelay, _MaxDelay);
            _MaxDelay = Math.Max(_MaxDelay - _speedupStepSize, _MinDelay * 2);
            await Task.Delay(updateRate);
        }
    }
    static async Task ReportPerformance(int milliseconds)
    {
        var config = new ProducerConfig { BootstrapServers = "broker:9092" };
        string filePath = "p" + _id + "_reports/msgcount.txt";
        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            while (_running)
            {
                var localTemp = tmpMsgCount;
                tmpMsgCount = 0;
                var perfMessage = _id + " " + localTemp.ToString() + " " + _topic;
                var deliveryResult = await producer.ProduceAsync("producer_performance", new Message<Null, string> { Value = perfMessage });
                WritePerf(filePath, (msgCount + 1).ToString());

                await Task.Delay(milliseconds);
            }
            await producer.ProduceAsync("producer_performance", new Message<Null, string> { Value = _id + " " + msgCount.ToString() + " " + _topic + " q" }); // signal production is over
            WritePerf(filePath, (msgCount + 1).ToString());
        }
    }
    static void ConsumeUpdates(CancellationTokenSource ctsToken)
    {
        var timeStamp = Stopwatch.GetTimestamp();

        var configuration = GetConsumerConfiguration();
        TimeSpan timeElapsed;
        using (var consumer = new ConsumerBuilder<Ignore, string>(configuration).Build())
        {
            consumer.Subscribe("p" + _id + "_producer_update");
            try
            {
                while (!ctsToken.IsCancellationRequested)
                {
                    timeElapsed = Stopwatch.GetElapsedTime(timeStamp);

                    double cpuUsage = timeElapsed.Ticks / (float)Stopwatch.Frequency * 100;
                    timeStamp = Stopwatch.GetTimestamp();
                    var consumeResult = consumer.Consume(ctsToken.Token);
                    if (consumeResult.Message == null)
                    {
                        Log.Information("no update");
                        continue;
                    }
                    Log.Information($"received {consumeResult.Message.Value} at {consumeResult.Message.Timestamp.UtcDateTime}");
                    string[] parts = consumeResult.Message.Value.Split(new char[] { ' ' });
                    for (int i = 1; i < parts.Length; i += 2)
                    {
                        switch (parts[i])
                        {
                            case "md": //throttle delay
                                if (!int.TryParse(parts[i + 1], out int md)) continue;
                                Log.Information($"========================= Updated md from {_throttleMinDelay} to {Math.Max(_throttleMinDelay + md, _MinDelay)} =========================");
                                _throttleMinDelay = Math.Max(_throttleMinDelay + md, _MinDelay);
                                break;
                            case "tt": //throttle time
                                if (!long.TryParse(parts[i + 1], out long tt)) continue;
                                Log.Information($"========================= Updated tt from {_throttleTime} to {_throttleTime + tt} =========================");
                                _throttleTime += tt;
                                break;
                            default: continue;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Information("OperationCanceledException caught: " + ex.Message);
                _running = false;
            }
            finally
            {
                Log.Information($"Closing consumer.");
                consumer.Close();
                _running = false;
            }
        }
    }
    static ConsumerConfig GetConsumerConfiguration()
    {
        return new ConsumerConfig
        {
            BootstrapServers = "broker:9092",
            GroupId = "p" + _id,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            EnablePartitionEof = true,
            StatisticsIntervalMs = 5000,
        };
    }
    static public void ConfigureLogging()
    {
        string logFileName = $"log_{DateTime.Now:yyyyMMdd_HHmmss}.txt";

        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .WriteTo.File(logFileName, rollingInterval: RollingInterval.Infinite)
            .CreateLogger();
    }
    static async Task CreateTopicAsync(string topicName)
    {
        using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "broker:9092" }).Build())
        {
            try
            {
                await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                        new TopicSpecification { Name = topicName, ReplicationFactor = 1, NumPartitions = 1 } });
            }
            catch (CreateTopicsException e)
            {
                Log.Information($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
            }
        }
    }
    static void WritePerf(string path, string info)
    {
        FileInfo fi = new FileInfo(path);
        if (!fi.Directory.Exists)
        {
            System.IO.Directory.CreateDirectory(fi.DirectoryName);
        }
        using (StreamWriter writer = new StreamWriter(path, true))
        {
            writer.WriteLine(info);
        }
    }
    static void ParseConfig()
    {
        var builder = new ConfigurationBuilder()
      .SetBasePath(AppContext.BaseDirectory)
      .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

        IConfiguration config = builder.Build();

        _id = config.GetValue<int>("ProducerSettigns:id");
        _topic = config.GetValue<string>("ProducerSettigns:topic");
        _runtime = config.GetValue<int>("ProducerSettigns:runtime");

        _MaxDelay = config.GetValue<int>("Dyconits:max_delay");
        _MinDelay = config.GetValue<int>("Dyconits:min_delay");
        _speedupStepSize = config.GetValue<int>("Dyconits:step_size");
    }
}
