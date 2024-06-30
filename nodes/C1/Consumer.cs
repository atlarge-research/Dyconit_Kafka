using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;
using System.Diagnostics;
using Confluent.Kafka.Admin;
using System.IO;
using Microsoft.Extensions.Configuration;

class Consumer
{
    private static int _id;
    private static Process _currentProcess;
    private static double _upperCpuThreshold;
    private static double _targetCpuUsage;
    private static double cpuUsage = 0;
    private static int _defaultNumErrBound;
    private static bool newBoudsNeeded = false;
    private static bool newBoudsReqSent = false;
    private static List<string> _topics = new List<string>();
    private static Dictionary<string, double> _topicPriorityLocalOffset = new Dictionary<string, double>();
    private static int _defaultPollDelay;
    private static int _minPollDelay;
    private static int _pollRateStep;
    private static int _maxPollDelay;
    private static int _reportInterval;
    private static long _defaultStaleErrBound;
    private static double _stalePriorityInc;
    private static int _messageCost;
    private static int _prioMessageCost;
    private static Dictionary<string, double> _topicPriority = new Dictionary<string, double>(); // -1 (lowest) to 1 (highest)
    private static Dictionary<string, int> topicPollDelay = new Dictionary<string, int>();
    private static Dictionary<string, int> topicConsumeCount = new Dictionary<string, int>();
    private static Dictionary<string, double> topicNumErrorBound = new Dictionary<string, double>();
    private static Dictionary<string, int> topicProduceCount = new Dictionary<string, int>();
    private static Dictionary<string, long> topicTimeStamp = new Dictionary<string, long>();
    private static Dictionary<string, double> topicStalePriorityOffset = new Dictionary<string, double>();
    private static Dictionary<string, double> _topicDefaultStalePriorityOffset = new Dictionary<string, double>();
    private static Dictionary<string, long> topicStaleErrBounds = new Dictionary<string, long>();
    private static Dictionary<string, int> _topicMaxPollDelay = new Dictionary<string, int>(); // the slowest each topic has to be refreshed
    private static Dictionary<string, CancellationTokenSource> _topicCancelToken = new Dictionary<string, CancellationTokenSource>();
    private static bool _ignoreDyconits = false;
    static async Task Main()
    {
        ParseConfig();

        _topics.Add("c" + _id + "_consumer_update");
        _topicPriorityLocalOffset.Add("c" + _id + "_consumer_update", 0);
        topicStalePriorityOffset.Add("c" + _id + "_consumer_update", 1);

        ConfigureLogging();
        var configuration = GetConsumerConfiguration();
        _currentProcess = Process.GetCurrentProcess();

        // Create a consumer task for each topic
        var consumerTasks = new List<Task>();
        var con_config = GetConsumerConfiguration();
        foreach (var topic in _topics)
        {
            await CreateTopicAsync(topic);
            _topicPriority.Add(topic, 0);
            topicPollDelay.Add(topic, _defaultPollDelay);
            topicConsumeCount.Add(topic, 0);
            _topicMaxPollDelay.Add(topic, _defaultPollDelay * 5);
            topicNumErrorBound.Add(topic, _defaultNumErrBound);
            topicProduceCount.Add(topic, 0);
            topicTimeStamp.Add(topic, 0);
            topicStaleErrBounds.Add(topic, _defaultStaleErrBound);
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };
            _topicCancelToken.Add(topic, cts);
            consumerTasks.Add(Task.Run(() => ConsumeMessages(topic, cts, con_config)));
        }
        consumerTasks.Add(Task.Run(() => ReportPerformance(_reportInterval)));
        await Task.WhenAll(consumerTasks);
    }
    static async Task ConsumeMessages(string topic, CancellationTokenSource token, ConsumerConfig consumerConfig)
    {
        using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
        {
            consumer.Subscribe(topic);
            var config = new ProducerConfig { BootstrapServers = "broker:9092" };
            var producer = new ProducerBuilder<Null, string>(config).Build();
            var pollFrequency = topicPollDelay[topic];

            var timeStamp = Stopwatch.GetTimestamp();
            bool first = true;
            TimeSpan timeElapsed;
            try
            {
                while (!token.Token.IsCancellationRequested)
                {
                    pollFrequency = topicPollDelay[topic];

                    timeElapsed = Stopwatch.GetElapsedTime(timeStamp);

                    var diff = pollFrequency - timeElapsed.TotalMilliseconds;
                    if (diff > 0 && !first && topic != "c" + _id + "_consumer_update")
                    {
                        await Task.Delay((int)diff);
                    }
                    first = false;
                    timeStamp = Stopwatch.GetTimestamp();

                    var consumeResult = consumer.Consume(token.Token);

                    if (consumeResult != null && consumeResult.Message != null && consumeResult.Message.Value != null)
                    {
                        Log.Information($"Received {topic} {consumeResult.Message.Timestamp.UtcDateTime} {consumeResult.Message.Value}");
                        topicConsumeCount[topic] += 1;
                        topicTimeStamp[topic] = consumeResult.Message.Timestamp.UnixTimestampMs;
                        var sim = 0;
                        if (topic == "topic_priority") { sim = _prioMessageCost; }
                        else if (topic == "topic_normal" || topic == "topic_low") { sim = _messageCost; }
                        for (int i = 0; i < sim; i++)
                        {
                            double result = Math.Sin(i) * Math.Cos(i);
                            Log.Information($"simulating usage {result}");
                        }
                    }
                    else
                        continue;

                    Log.Information($"{topic} consume count: {topicConsumeCount[topic]}");

                    if (topic == "c" + _id + "_consumer_update")
                    {
                        string[] parts = consumeResult.Message.Value.Split(new char[] { ' ' });
                        for (int j = 1; j < parts.Length; j += (_topics.Count - 1) * 2 + 1)
                        {
                            switch (parts[j])
                            {
                                case "mp": // min poll rate
                                    for (int i = j + 1; i < (_topics.Count - 1) * 2 + j + 1; i += 2)
                                    {
                                        if (!int.TryParse(parts[i + 1], out int mp)) continue;
                                        _topicMaxPollDelay[parts[i]] = mp;
                                        Log.Information($"Updated MP to {mp} at {parts[i]}");
                                    }
                                    break;
                                case "tp": // topic priority
                                    for (int i = j + 1; i < (_topics.Count - 1) * 2 + j + 1; i += 2)
                                    {
                                        if (!double.TryParse(parts[i + 1], out double tp)) continue;
                                        _topicPriority[parts[i]] = tp;
                                        if (tp == -2)
                                        {
                                            _topicCancelToken[parts[i]].Cancel();
                                            Log.Information($"Closing {parts[i]}");
                                        }
                                        Log.Information($"Updated TP to {tp} at {parts[i]}");
                                    }
                                    break;
                                case "pc": // produce count
                                    for (int i = j + 1; i < (_topics.Count - 1) * 2 + j + 1; i += 2)
                                    {
                                        if (!int.TryParse(parts[i + 1], out int pc)) continue;
                                        topicProduceCount[parts[i]] = pc;
                                        Log.Information($"Updated PC to {pc} at {parts[i]}");
                                    }
                                    break;
                                case "bn": //bound num err
                                    for (int i = j + 1; i < (_topics.Count - 1) * 2 + j + 1; i += 2)
                                    {
                                        if (!double.TryParse(parts[i + 1], out double nb)) continue;
                                        topicNumErrorBound[parts[i]] += Math.Floor(nb);
                                        Log.Information($"Updated BN by {nb} {topicNumErrorBound[parts[i]]} at {parts[i]}");
                                    }
                                    break;
                                case "bs": //bound staleness err
                                    for (int i = j + 1; i < (_topics.Count - 1) * 2 + j + 1; i += 2)
                                    {
                                        if (!double.TryParse(parts[i + 1], out double bs)) continue;
                                        topicStaleErrBounds[parts[i]] += (long)Math.Floor(bs);
                                        topicStalePriorityOffset[parts[i]] = _topicDefaultStalePriorityOffset[parts[i]];
                                        Log.Information($"Updated BS by {bs} {topicStaleErrBounds[parts[i]]} at {parts[i]}");
                                    }
                                    break;
                                default: continue;
                            }

                        }
                        newBoudsReqSent = false;
                        NormalizePriorities();
                        Task task = Task.Run(() => DeterminePollRate());
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Information("Exception caught: " + ex.Message);
            }
            finally
            {
                Log.Information($"Closing consumer. {topic}");
                consumer.Close();
            }
        }
    }
    static async Task ReportPerformance(int milliseconds)
    {
        var config = new ProducerConfig { BootstrapServers = "broker:9092" };
        double prevTime = 0;
        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            while (IsConsumptionRunning())
            {
                double cpuMsTotal = _currentProcess.TotalProcessorTime.TotalMilliseconds;
                double newTime = cpuMsTotal - prevTime;
                prevTime = cpuMsTotal;
                cpuUsage = newTime / milliseconds * 100;
                if (cpuUsage > _upperCpuThreshold && !newBoudsReqSent)
                {
                    Log.Information($"CPU threshold exceeded by {cpuUsage - _upperCpuThreshold}");
                    newBoudsNeeded = true;
                }
                List<Task> writeTasks = new List<Task>();
                string perfMessage = _id + " " + cpuUsage;
                string filePath = "c" + _id + "_reports/cpu.txt";
                writeTasks.Add(Task.Run(() => WritePerf(filePath, cpuUsage.ToString())));
                for (int i = 0; i < _topics.Count; i++)
                {
                    var filePathMsgCount = "c" + _id + "_reports/msgcount_" + _topics[i] + ".txt";
                    var tmp = i;
                    writeTasks.Add(Task.Run(() => WritePerf(filePathMsgCount, topicConsumeCount[_topics[tmp]].ToString())));
                    if (_topics[i] == "c" + _id + "_consumer_update") continue;
                    perfMessage += " " + _topics[i] + " " + topicConsumeCount[_topics[i]];
                }
                if (newBoudsNeeded)
                {
                    perfMessage += " nb";
                    newBoudsNeeded = false;
                    newBoudsReqSent = true;
                }
                var deliveryResult = producer.ProduceAsync("consumer_performance", new Message<Null, string> { Value = perfMessage });
                await Task.WhenAll(writeTasks);
                Task task = Task.Run(() => DeterminePollRate());
                await Task.Delay(milliseconds);
            }
            _topicCancelToken["c" + _id + "_consumer_update"].Cancel();
        }
    }
    static ConsumerConfig GetConsumerConfiguration()
    {
        return new ConsumerConfig
        {
            BootstrapServers = "broker:9092",
            GroupId = "c" + _id,
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
    static void DeterminePollRate()
    {
        double updateStep = (cpuUsage - _targetCpuUsage) / Math.Abs(cpuUsage - _targetCpuUsage) * _pollRateStep;

        foreach (var topic in _topics)
        {
            if (_ignoreDyconits)
            {
                Log.Information($" Ignoring error bounds");
                topicPollDelay[topic] = _minPollDelay;
                continue;
            }
            if (_topicPriority[topic] <= -1)
            {
                topicPollDelay[topic] = _maxPollDelay;
            }
            else
            if (IsStaleErrorExceeded(topic))
            {
                Log.Information($"Staleness error bound exceeded");
                topicPollDelay[topic] = (int)Math.Floor(Math.Max(Math.Min(topicPollDelay[topic] + (int)updateStep - (_pollRateStep * (_topicPriority[topic] + topicStalePriorityOffset[topic])), _maxPollDelay), _minPollDelay));
                topicStalePriorityOffset[topic] = Math.Min(topicStalePriorityOffset[topic] + _stalePriorityInc, (_topicDefaultStalePriorityOffset[topic] + 1) * 3);
            }
            else
            if (IsNumErrorExceeded(topic))
            {
                Log.Information($"Numerical error bound exceeded");
                topicPollDelay[topic] = (int)Math.Floor(Math.Max(Math.Min(topicPollDelay[topic] + (int)updateStep - (_pollRateStep * _topicPriority[topic]), _topicMaxPollDelay[topic]), _minPollDelay));
            }
            else
            {
                topicPollDelay[topic] = (int)Math.Floor(Math.Max(Math.Min(topicPollDelay[topic] + (int)updateStep - (_pollRateStep * _topicPriority[topic]), _maxPollDelay), _minPollDelay));
            }

        }
        topicPollDelay["c" + _id + "_consumer_update"] = 0;
    }
    static bool IsStaleErrorExceeded(string topic)
    {
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - topicTimeStamp[topic] > topicStaleErrBounds[topic] && topicProduceCount[topic] != topicConsumeCount[topic];
    }
    static bool IsNumErrorExceeded(string topic)
    {
        return topicProduceCount[topic] - topicConsumeCount[topic] > topicNumErrorBound[topic];
    }
    static bool IsConsumptionRunning()
    {
        foreach (var cancelToken in _topicCancelToken)
        {
            if (cancelToken.Key == "c" + _id + "_consumer_update")
                continue;
            if (!cancelToken.Value.IsCancellationRequested)
            {
                return true;
            }
        }
        return false;
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
    static void NormalizePriorities()
    {
        foreach (var topic in _topics)
        {
            if (_topicPriority[topic] == -1 || _topicPriority[topic] == -2) continue;
            _topicPriority[topic] += _topicPriorityLocalOffset[topic];
        }

        _topicPriority["c" + _id + "_consumer_update"] = -5;
        var maxprioritypair = _topicPriority.OrderByDescending(kvp => kvp.Value).First();
        var maxpriority = maxprioritypair.Value;
        if (maxpriority == 0)
        {
            _topicPriority[maxprioritypair.Key] = 1;
            maxpriority = 1;
        }

        foreach (var topic in _topics)
        {
            if (_topicPriority[topic] == -1 || _topicPriority[topic] == -2) continue;
            _topicPriority[topic] = (_topicPriority[topic] + 1) / (maxpriority + 1);
        }
    }
    static void ParseConfig()
    {
        var builder = new ConfigurationBuilder()
      .SetBasePath(AppContext.BaseDirectory)
      .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

        IConfiguration config = builder.Build();

        _id = config.GetValue<int>("ConsumerSettings:id");
        _defaultPollDelay = config.GetValue<int>("ConsumerSettings:default_poll_delay");
        _minPollDelay = config.GetValue<int>("ConsumerSettings:min_poll_delay");
        _pollRateStep = config.GetValue<int>("ConsumerSettings:poll_rate_step");
        _maxPollDelay = config.GetValue<int>("ConsumerSettings:max_poll_delay");
        _reportInterval = config.GetValue<int>("ConsumerSettings:report_interval");
        _messageCost = config.GetValue<int>("ConsumerSettings:message_cost");
        _prioMessageCost = config.GetValue<int>("ConsumerSettings:priority_message_cost");
        _ignoreDyconits = config.GetValue<bool>("ConsumerSettings:ignore_dyconits");

        _upperCpuThreshold = config.GetValue<double>("Dyconits:cons_cpu_up_threshold");
        _targetCpuUsage = config.GetValue<double>("Dyconits:cpu_target_usage");
        _defaultNumErrBound = config.GetValue<int>("Dyconits:num_error_bound");
        _defaultStaleErrBound = config.GetValue<int>("Dyconits:stale_error_bound");
        _stalePriorityInc = config.GetValue<double>("Dyconits:stale_priority_increment");

        config.GetSection("Collections:topics").Bind(_topics);
        config.GetSection("Collections:topic_priority_offset").Bind(_topicPriorityLocalOffset);
        config.GetSection("Collections:stale_priority").Bind(topicStalePriorityOffset);
        config.GetSection("Collections:stale_priority").Bind(_topicDefaultStalePriorityOffset);
    }
}
