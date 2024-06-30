using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;
using System.Diagnostics;
using System.IO;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;


class Manager
{

    private static Dictionary<string, long> _msgCountPerTopic = new Dictionary<string, long>();
    private static Dictionary<string, double> _msgProdRatePerTopic = new Dictionary<string, double>(); // msgs per second in topic
    private static Dictionary<string, long> _updateTimeStampPerTopic = new Dictionary<string, long>();
    private static Dictionary<string, int> topicConsumeCount = new Dictionary<string, int>();
    private static Dictionary<string, double> _defaultTopicPriority = new Dictionary<string, double>();
    // arbitrary custom topic priority
    // -1 reserved for no priorotiy topic
    // -2 reserved for non-producting topics
    private static Dictionary<string, double> _currentTopicPriority = new Dictionary<string, double>(); // actuall priority taking prod rate and settings into account
    private static Dictionary<string, bool> _isTopicActive = new Dictionary<string, bool>();
    private static Dictionary<int, bool> _consumerReqNewBounds = new Dictionary<int, bool>();
    private static Dictionary<string, HashSet<int>> _topicProducers = new Dictionary<string, HashSet<int>>();
    private static Process _currentProcess;
    static HashSet<int> _consumers = new HashSet<int>();
    static Dictionary<int, Dictionary<string, int>> _consumerConsumeCount = new Dictionary<int, Dictionary<string, int>>();
    private static int _numErrBoundIncStep;
    private static long _staleErrBoundIncStep;
    private static double _disableTopicPrioThreshold;
    private static bool _canDisableTopics;
    private static bool _canTurnOffTopics;
    private static bool _canThrottleProducers;
    private static int _producerThrottleDelay;
    private static List<string> _producedTopics = new List<string>();
    private static List<string> _consumedTopics = new List<string>();
    public static long _producerThrottleTime;
    private static double _throttleThreshold;
    private static double _notifyConsumersThreshold;


    static async Task Main()
    {
        ParseConfig();
        ConfigureLogging();
        var configuration = GetConsumerConfiguration();

        _currentProcess = Process.GetCurrentProcess();

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        foreach (var topic in _producedTopics)
        {
            _currentTopicPriority.Add(topic, _defaultTopicPriority[topic]);
            _msgCountPerTopic.Add(topic, 0);
            _msgProdRatePerTopic.Add(topic, 0);
            _updateTimeStampPerTopic.Add(topic, 0);
            _isTopicActive.Add(topic, false);
            _topicProducers.Add(topic, new HashSet<int>());
        }

        var consumerTasks = new List<Task>();
        foreach (var topic in _consumedTopics)
        {
            await CreateTopicAsync(topic);
            topicConsumeCount.Add(topic, 0);
            consumerTasks.Add(Task.Run(() => ConsumeMessages(topic, cts.Token, configuration)));
        }
        await Task.WhenAll(consumerTasks);
    }
    static async Task SendUpdate(string message, string topic)
    {
        var config = new ProducerConfig { BootstrapServers = "broker:9092" };
        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            var deliveryResult = await producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
            Log.Information($"Sending {topic} || {message}");
        }
    }
    static void ConsumeMessages(string topic, CancellationToken token, ConsumerConfig configuration)
    {
        using (var consumer = new ConsumerBuilder<Ignore, string>(configuration).Build())
        {
            consumer.Subscribe(topic);
            try
            {
                while (!token.IsCancellationRequested)
                {
                    double cpuUsage = _currentProcess.TotalProcessorTime.Ticks / (float)Stopwatch.Frequency * 100;

                    var consumeResult = consumer.Consume(token);
                    if (consumeResult != null && consumeResult.Message != null && consumeResult.Message.Value != null)
                    {
                        var inputMessage = consumeResult.Message.Value;
                        topicConsumeCount[topic] += 1;
                    }
                    else
                    {
                        Log.Warning($"===================== Currently no message in topic {topic} =====================");
                        continue;
                    }
                    Log.Information($"{topic} consume count: {topicConsumeCount[topic]}");
                    Log.Information($"Received {topic} {consumeResult.Message.Timestamp.UtcDateTime} {consumeResult.Message.Value}");

                    string[] parts = consumeResult.Message.Value.Split(new char[] { ' ' });
                    if (topic == "consumer_performance")
                    {
                        Task task = Task.Run(() => procesConReps(parts));
                    }
                    else
                    {
                        Task task = Task.Run(() => ProcesProdReps(parts, consumeResult.Message.Timestamp.UnixTimestampMs));
                    }

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception caught: " + ex.Message);
                consumer.Close();
            }
            finally
            {
                Log.Information($"Closing consumer. {topic}");
                consumer.Close();
            }
        }
    }
    static void ProcesProdReps(string[] report, long timeStamp)
    {
        if (int.TryParse(report[0], out int proId) && int.TryParse(report[1], out int proPerf) && report.Length > 2)
        {
            string proTopic = report[2];
            if (_producedTopics.IndexOf(proTopic) == -1)
            {
                _producedTopics.Add(proTopic);
                _currentTopicPriority.Add(proTopic, 0);
                _msgCountPerTopic.Add(proTopic, 0);
                _msgProdRatePerTopic.Add(proTopic, 0);
                _updateTimeStampPerTopic.Add(proTopic, 0);
                _isTopicActive.Add(proTopic, true);
                _topicProducers.Add(proTopic, new HashSet<int>());
            }
            _topicProducers[proTopic].Add(proId);

            if (!_isTopicActive[proTopic])
            {
                _isTopicActive[proTopic] = true;
            }
            if (report[^1] == "q")
            {
                Log.Information($"producer id {proId} has quit the topic {proTopic}");
                _msgProdRatePerTopic[proTopic] = 0;
                _msgCountPerTopic[proTopic] = proPerf;
                UpdateCurrPriorityDic();
                _isTopicActive[proTopic] = false;
                return;
            }
            _msgCountPerTopic[proTopic] = _msgCountPerTopic[proTopic] + proPerf;
            double proRate = (double)proPerf / (timeStamp - _updateTimeStampPerTopic[proTopic]) * 1000;
            var percentageIncrease = proRate / _msgProdRatePerTopic[proTopic];
            _msgProdRatePerTopic[proTopic] = proRate;
            _updateTimeStampPerTopic[proTopic] = timeStamp;
            WritePerf("m_reports/producers/p" + proId + ".txt", proRate.ToString());
            UpdateCurrPriorityDic();
            if (percentageIncrease < 1 - _notifyConsumersThreshold || percentageIncrease > 1 + _notifyConsumersThreshold)
            {
                Log.Information($"notifying consumers");
                NotifyConsumers();
            }
        }
        else
        {
            Log.Warning($"Wrong message format");
        }
    }

    static void NotifyConsumers()
    {
        foreach (var consumer in _consumers)
        {
            RunPerfCheck(consumer, GenerateUpdates);
        }
    }

    static async Task GenerateUpdates(int id)
    {
        List<string> conTopcis = new List<string>();
        List<long> consumeCount = new List<long>();
        List<double> priority = new List<double>();
        bool throttled = false;

        if (ShouldThrottle())
        {
            foreach (var topic in _producedTopics)
            {
                if (!CanDisableTopic(topic)) continue;
                foreach (var producer in _topicProducers[topic])
                {
                    throttled = true;
                    await SendUpdate("p" + producer + " md " + _producerThrottleDelay + " tt " + _producerThrottleTime, "p" + producer + "_producer_update");
                }
            }
        }

        if (throttled)
        {
            foreach (var consumer in _consumerReqNewBounds.Keys)
            {
                _consumerReqNewBounds[consumer] = false;
            }
        }

        foreach (var topic in _consumerConsumeCount[id])
        {
            conTopcis.Add(topic.Key);

            Log.Debug($"checking {topic.Key} {topic.Value} {_msgCountPerTopic[topic.Key]}");

            if (_msgCountPerTopic[topic.Key] - topic.Value == 0 && _msgCountPerTopic[topic.Key] != 0 && !_isTopicActive[topic.Key] && _canTurnOffTopics)
            {
                priority.Add(-2);
                Log.Information($"Close yourself at {topic.Key}");
            }
            else
            {
                if (CanDisableTopic(topic.Key) && _consumerReqNewBounds[id] && _canDisableTopics)
                {
                    Log.Information($"Disable {topic.Key}");
                    priority.Add(-1);
                }
                else
                    priority.Add(_currentTopicPriority[topic.Key]);
            }
            consumeCount.Add(_msgCountPerTopic[topic.Key]);
        }
        await SendUpdate(ConstructConsumerPerfUpdate(id, conTopcis, priority), "c" + id + "_consumer_update");

    }
    static void procesConReps(string[] report)
    {
        if (int.TryParse(report[0], out int consumerId) && double.TryParse(report[1], out double consumerPerf) && report.Length > 2)
        {
            if (!_consumers.Contains(consumerId))
            {
                _consumers.Add(consumerId);
                _consumerConsumeCount.Add(consumerId, new Dictionary<string, int>());
            }
            if (!_consumerReqNewBounds.ContainsKey(consumerId))
            {
                _consumerReqNewBounds.Add(consumerId, false);
            }
            if (report[^1] == "nb")
                _consumerReqNewBounds[consumerId] = true;
            else
                _consumerReqNewBounds[consumerId] = false;
            for (int i = 2; i < report.Length - 1; i += 2)
            {
                if (!int.TryParse(report[i + 1], out int cc)) continue;
                if (!_consumerConsumeCount[consumerId].ContainsKey(report[i]))
                    _consumerConsumeCount[consumerId].Add(report[i], 0);
                if (_producedTopics.IndexOf(report[i]) == -1)
                {
                    _producedTopics.Add(report[i]);
                    _currentTopicPriority.Add(report[i], 0);
                    _msgCountPerTopic.Add(report[i], 0);
                    _msgProdRatePerTopic.Add(report[i], 0);
                    _updateTimeStampPerTopic.Add(report[i], 0);
                    _isTopicActive.Add(report[i], false);
                }
                _consumerConsumeCount[consumerId][report[i]] = cc;
            }

            RunPerfCheck(consumerId, GenerateUpdates);
            Task task = Task.Run(() => WritePerf("m_reports/consumers/c" + consumerId + ".txt", consumerPerf.ToString()));
        }
        else
        {
            Log.Warning($"Wrong message format");
        }
    }
    static void RunPerfCheck(int id, Func<int, Task> func)
    {
        try
        {
            Task task = Task.Run(() => func(id));
        }
        catch (InvalidOperationException ex)
        {
            Console.WriteLine("InvalidOperationException caught: " + ex.Message);
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

    static ConsumerConfig GetConsumerConfiguration()
    {
        return new ConsumerConfig
        {
            BootstrapServers = "broker:9092",
            GroupId = "m1",
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
                Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
            }
        }
    }

    static void UpdateCurrPriorityDic()
    {
        var maxProdRatePair = _msgProdRatePerTopic.OrderByDescending(kvp => kvp.Value).First();
        var maxProdRate = maxProdRatePair.Value;
        if (maxProdRate == 0)
            maxProdRate = 1;
        foreach (var topic in _producedTopics)
        {
            if (_currentTopicPriority[topic] == -1) continue;
            _currentTopicPriority[topic] = (_msgProdRatePerTopic[topic] / maxProdRate * 2 - 1 + _defaultTopicPriority[topic]) / 2;
            Log.Information($"{topic} has priority {_currentTopicPriority[topic]}");
        }
    }
    static string ConstructConsumerPerfUpdate(int id, List<string> topics, List<double> priority)
    {
        string report = $"c{id} mp";

        foreach (var topic in topics)
        {
            if (_msgProdRatePerTopic[topic] == 0)
            {
                report += " " + topic + " " + 2500;
                continue;
            }
            report += " " + topic + " " + (int)(1000 / _msgProdRatePerTopic[topic]);
        }
        report += " tp";
        for (int i = 0; i < topics.Count; i++)
        {
            report += " " + topics[i] + " " + priority[i];
        }
        report += " pc";
        foreach (var topic in topics)
        {
            report += " " + topic + " " + _msgCountPerTopic[topic];
        }

        if (_consumerReqNewBounds[id])
        {
            report += " bn";
            foreach (var topic in topics)
            {
                double newBound = _numErrBoundIncStep + _numErrBoundIncStep * (1 - (1 + _currentTopicPriority[topic]) / 2);
                report += " " + topic + " " + newBound;
            }
            report += " bs";
            foreach (var topic in topics)
            {
                double newBound = _staleErrBoundIncStep + _staleErrBoundIncStep * (1 - (1 + _currentTopicPriority[topic]) / 2);
                report += " " + topic + " " + newBound;
            }
        }
        return report;
    }

    static bool ShouldThrottle()
    {
        var numOfStruggling = 0.0;
        var totalConsumers = 0.0;
        foreach (var consumer in _consumerReqNewBounds)
        {
            totalConsumers++;
            if (!consumer.Value) continue;
            numOfStruggling++;
        }
        return ((numOfStruggling / totalConsumers) > _throttleThreshold) && _canThrottleProducers;
    }
    static bool CanDisableTopic(string topic)
    {
        var maxpriority = _currentTopicPriority.OrderByDescending(kvp => kvp.Value).First();
        return maxpriority.Value - _currentTopicPriority[topic] > _disableTopicPrioThreshold;
    }

    static void ParseConfig()
    {
        var builder = new ConfigurationBuilder()
      .SetBasePath(AppContext.BaseDirectory)
      .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

        IConfiguration config = builder.Build();

        _canDisableTopics = config.GetValue<bool>("MangerSettings:can_disable");
        _canTurnOffTopics = config.GetValue<bool>("MangerSettings:can_turnoff");
        _canThrottleProducers = config.GetValue<bool>("MangerSettings:can_throttle");

        _disableTopicPrioThreshold = config.GetValue<double>("Dyconits:disable_prio_thresh");
        _numErrBoundIncStep = config.GetValue<int>("Dyconits:num_err_bound_inc_step");
        _staleErrBoundIncStep = config.GetValue<int>("Dyconits:stale_err_bound_inc_step");
        _producerThrottleDelay = config.GetValue<int>("Dyconits:producer_throttle_delay");
        _producerThrottleTime = config.GetValue<long>("Dyconits:producer_throttle_time");
        _throttleThreshold = config.GetValue<double>("Dyconits:struggling_consumer_threshold_throttle_");
        _notifyConsumersThreshold = config.GetValue<double>("Dyconits:production_rate_change_bound_update_threshold");

        config.GetSection("Collections:produced_topics").Bind(_producedTopics);
        config.GetSection("Collections:consumed_topics").Bind(_consumedTopics);
        config.GetSection("Collections:set_topic_priority").Bind(_defaultTopicPriority);
    }
}
