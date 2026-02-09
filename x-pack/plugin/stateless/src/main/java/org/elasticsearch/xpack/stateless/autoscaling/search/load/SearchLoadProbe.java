/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.autoscaling.search.load;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.stateless.autoscaling.MetricQuality;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.stateless.StatelessPlugin.SHARD_READ_THREAD_POOL;
import static org.elasticsearch.xpack.stateless.autoscaling.search.load.AverageSearchLoadSampler.SEARCH_EXECUTOR;

/**
 * This class computes the current node search load
 */
public class SearchLoadProbe {

    public static final TimeValue DEFAULT_MAX_TIME_TO_CLEAR_QUEUE = TimeValue.timeValueSeconds(15);

    /**
     * MAX_TIME_TO_CLEAR_QUEUE is a threshold that defines the length of time that the current number of threads could take to clear the
     * queued work. In other words, the amount of work that is considered manageable using the current number of threads. For example,
     * MAX_TIME_TO_CLEAR_QUEUE = 30sec means, 30 seconds worth of tasks in the queue is considered manageable with the current number of
     * threads available.
     * <p>
     * Setting `MAX_TIME_TO_CLEAR_QUEUE = 0` disables using queuing in load calculations.
     */
    public static final Setting<TimeValue> MAX_TIME_TO_CLEAR_QUEUE = Setting.timeSetting(
        "serverless.autoscaling.search.sampler.max_time_to_clear_queue",
        DEFAULT_MAX_TIME_TO_CLEAR_QUEUE,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    /**
     * This setting is used to determine the threshold of load in the SHARD_READ_EXECUTOR under which the Search Load is considered valid.
     * If the {@code shardReadLoad < SHARD_READ_LOAD_THRESHOLD_SETTING}, the Search Load is considered valid, as the SEARCH_EXECUTOR is not
     * considered to be blocked on the blob-store contents being downloaded by the SHARD_READ_EXECUTOR.
     */
    public static final Setting<Double> SHARD_READ_LOAD_THRESHOLD_SETTING = Setting.doubleSetting(
        "serverless.autoscaling.search.shard_read_load.threshold",
        0.1,
        0.0,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    /**
     * The amount that we'll allow the queue contribution to be relative to the size of the current node. Default value of 3 means 3*thread
     * count, i.e., a max scale up of 4x.
     */
    public static final Setting<Float> MAX_QUEUE_CONTRIBUTION_FACTOR = Setting.floatSetting(
        "serverless.autoscaling.search.sampler.max_queue_contribution_factor",
        3,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    public static final double DEFAULT_SCALING_FACTOR = 1.05;
    /**
     * A scaling factor applied to the calculated search load to provide some buffer capacity above the calculated load so we can absorbe
     * spikes in load without immediately scaling up. 1.05 means a 5% buffer.
     * This is currently only applied to the search load for the search thread pool.
     */
    public static final Setting<Double> SEARCH_LOAD_SCALING_FACTOR = Setting.doubleSetting(
        "serverless.autoscaling.search.load.scaling_factor",
        DEFAULT_SCALING_FACTOR,
        1.00,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    private final Function<String, ExecutorLoadStats> searchExecutorStatsProvider;
    private volatile TimeValue maxTimeToClearQueue;
    private volatile float maxQueueContributionFactor;
    private volatile double shardReadLoadThreshold;
    private volatile double searchLoadScalingFactor;
    private final Map<String, ThreadPoolMetrics> threadPoolMetricsMap;

    @SuppressWarnings("this-escape")
    public SearchLoadProbe(
        ClusterSettings clusterSettings,
        Function<String, ExecutorLoadStats> searchExecutorStatsProvider,
        MeterRegistry meterRegistry
    ) {
        this.searchExecutorStatsProvider = searchExecutorStatsProvider;
        this.threadPoolMetricsMap = Map.of(SEARCH_EXECUTOR, new ThreadPoolMetrics(), SHARD_READ_THREAD_POOL, new ThreadPoolMetrics());
        registerMetrics(meterRegistry);

        clusterSettings.initializeAndWatch(MAX_TIME_TO_CLEAR_QUEUE, this::setMaxTimeToClearQueue);
        clusterSettings.initializeAndWatch(MAX_QUEUE_CONTRIBUTION_FACTOR, this::setMaxQueueContributionFactor);
        clusterSettings.initializeAndWatch(SHARD_READ_LOAD_THRESHOLD_SETTING, this::setShardReadLoadThreshold);
        clusterSettings.initializeAndWatch(SEARCH_LOAD_SCALING_FACTOR, this::setSearchLoadScalingFactor);
    }

    private void registerMetrics(MeterRegistry meterRegistry) {
        for (Map.Entry<String, ThreadPoolMetrics> entry : threadPoolMetricsMap.entrySet()) {
            String threadPoolName = entry.getKey();
            ThreadPoolMetrics metrics = entry.getValue();

            meterRegistry.registerLongGauge(
                "es.autoscaling.search_load." + threadPoolName + ".queue_backlog_duration.current",
                "The duration we had queuing in the threadpool",
                "ns",
                () -> new LongWithAttributes(metrics.lastQueueBacklogDurationNanos)
            );
            meterRegistry.registerDoubleGauge(
                "es.autoscaling.search_load." + threadPoolName + ".task_avg_execution_time.current",
                "The average task execution time",
                "ns",
                () -> new DoubleWithAttributes(metrics.averageTaskExecutionTime)
            );
            meterRegistry.registerDoubleGauge(
                "es.autoscaling.search_load." + threadPoolName + ".queue_clear_threads_required.current",
                "The number of threads needed to clear the current queue in an acceptable amount of time",
                "1",
                () -> new DoubleWithAttributes(metrics.queueThreadsNeeded)
            );
            meterRegistry.registerDoubleGauge(
                "es.autoscaling.search_load." + threadPoolName + ".busy_threads.current",
                "The average number of threads busy handling work in the thread pool per second.",
                "1",
                () -> new DoubleWithAttributes(metrics.threadsUsed)
            );
            meterRegistry.registerDoubleGauge(
                "es.autoscaling.search_load." + threadPoolName + ".threadpool_load.current",
                "The estimated number of processors needed to handle currently active (non queued) work",
                "1",
                () -> new DoubleWithAttributes(metrics.threadPoolLoad)
            );
            meterRegistry.registerDoubleGauge(
                "es.autoscaling.search_load." + threadPoolName + ".queue_load.current",
                "The estimated number of processors needed to handle queued work within an acceptable amount of time",
                "1",
                () -> new DoubleWithAttributes(metrics.queueLoad)
            );
        }
    }

    /**
     * Reports the quality of the search load. The quality is only considered MetricQuality.EXACT when the SEARCH_EXECUTOR is not
     *  blocked on the blob-store contents being downloaded by the SHARD_READ_EXECUTOR (The SHARD_READ_EXECUTOR load is below the
     *  SHARD_READ_LOAD_THRESHOLD_SETTING).
     */
    public MetricQuality getSearchLoadQuality() {
        var shardReadLoad = getExecutorLoad(SHARD_READ_THREAD_POOL);
        return (shardReadLoad < shardReadLoadThreshold) ? MetricQuality.EXACT : MetricQuality.MINIMUM;
    }

    /**
     * Returns the current search load (number of processors needed to cope with the current search workload).
     */
    public double getSearchLoad() {
        return searchLoadScalingFactor * getExecutorLoad(SEARCH_EXECUTOR);
    }

    private double getExecutorLoad(String executorName) {
        assert threadPoolMetricsMap.containsKey(executorName) : "unsuported executor name: " + executorName;
        var searchExecutorStats = searchExecutorStatsProvider.apply(executorName);
        var metrics = threadPoolMetricsMap.get(executorName);
        return calculateSearchLoadForExecutor(
            searchExecutorStats.threadsUsed(),
            searchExecutorStats.averageTaskExecutionEWMA(),
            searchExecutorStats.currentQueueSize(),
            maxTimeToClearQueue,
            searchExecutorStats.queueBacklogDurationNanos(),
            maxQueueContributionFactor * searchExecutorStats.maxThreads(),
            searchExecutorStats.numProcessors(),
            searchExecutorStats.maxThreads(),
            metrics::update
        );
    }

    /**
     * Calculates the "Search Load", defined as the number of processors needed to cope with the work on this executor, both running
     * and queued (within the timeframe set by the maxTimeToClearQueue, and bounded by maxThreadsToHandleQueue).
     *
     * @param threadsUsed              The EWMA number of threads busy within the past 1 minute.
     * @param averageTaskExecutionTime The EWMA time/thread reported by the executor.
     * @param currentQueueSize         The queue size sampled at the current instant.
     * @param maxTimeToClearQueue      A setting that indicates the acceptable max time to clear the queue.
     * @param queueBacklogDurationNanos     The duration we've had queuing in the threadpool for.
     * @param maxThreadsToHandleQueue  The maximum number of threads that can be requested to handled queued work.
     * @return The number of threads needed to cope with the current search workload on this executor.
     */
    static double calculateSearchLoadForExecutor(
        double threadsUsed,
        double averageTaskExecutionTime,
        long currentQueueSize,
        TimeValue maxTimeToClearQueue,
        long queueBacklogDurationNanos,
        double maxThreadsToHandleQueue,
        double numProcessors,
        int threadPoolSize,
        Consumer<SearchLoadMetricsProvider> searchLoadMetricsConsumer
    ) {
        assert maxThreadsToHandleQueue > 0.0;
        assert threadPoolSize > 0;

        var maxTimeToClearQueueNanos = maxTimeToClearQueue.nanos();

        double processorPerThread = numProcessors / threadPoolSize;
        double threadPoolLoad = threadsUsed * processorPerThread;

        // Protect from divide by zero errors.
        if (averageTaskExecutionTime == 0.0 || maxTimeToClearQueueNanos == 0L) {
            // As the averageTaskExecutionEWMA approaches zero, the effect of the queue approaches zero, so it is valid to
            // simply report the threadPoolLoad without the queueLoad. A maxTimeToClearQueueNanos==0 disables the queuing contribution,
            // so we simply report threadPoolLoad.
            return threadPoolLoad;
        }

        // Determine the total threads needed to clear the queue in an acceptable timeframe,
        // given the speed at which threads are currently working, limited by the maxThreadsToHandleQueue.
        double tasksManageablePerThreadWithinMaxTime = maxTimeToClearQueueNanos / averageTaskExecutionTime;
        double queueThreadsNeeded = Math.min(currentQueueSize / tasksManageablePerThreadWithinMaxTime, maxThreadsToHandleQueue);
        assert queueThreadsNeeded >= 0.0;
        double queueLoad = queueThreadsNeeded * processorPerThread;
        searchLoadMetricsConsumer.accept(
            new SearchLoadMetricsProvider(
                averageTaskExecutionTime,
                queueThreadsNeeded,
                queueLoad,
                threadPoolLoad,
                queueBacklogDurationNanos,
                threadsUsed
            )
        );
        return threadPoolLoad + queueLoad;
    }

    void setMaxTimeToClearQueue(TimeValue maxTimeToClearQueue) {
        this.maxTimeToClearQueue = maxTimeToClearQueue;
    }

    void setMaxQueueContributionFactor(float maxQueueContributionFactor) {
        this.maxQueueContributionFactor = maxQueueContributionFactor;
    }

    void setShardReadLoadThreshold(double shardReadLoadThreshold) {
        this.shardReadLoadThreshold = shardReadLoadThreshold;
    }

    private void setSearchLoadScalingFactor(double newValue) {
        searchLoadScalingFactor = newValue;
    }

    // visible for testing
    double searchLoadScalingFactor() {
        return searchLoadScalingFactor;
    }

    record SearchLoadMetricsProvider(
        double averageTaskExecutionTime,
        double queueThreadsNeeded,
        double queueLoad,
        double threadPoolLoad,
        long lastQueueBacklogDurationNanos,
        double threadsUsed
    ) {}

    private static class ThreadPoolMetrics {
        volatile double averageTaskExecutionTime = 0.0;
        volatile double queueThreadsNeeded = 0.0;
        volatile double queueLoad = 0.0;
        volatile double threadPoolLoad = 0.0;
        volatile double threadsUsed = 0.0;
        volatile long lastQueueBacklogDurationNanos = 0L;

        void update(SearchLoadMetricsProvider provider) {
            this.averageTaskExecutionTime = provider.averageTaskExecutionTime;
            this.queueLoad = provider.queueLoad;
            this.queueThreadsNeeded = provider.queueThreadsNeeded;
            this.threadPoolLoad = provider.threadPoolLoad;
            this.lastQueueBacklogDurationNanos = provider.lastQueueBacklogDurationNanos;
            this.threadsUsed = provider.threadsUsed;
        }
    }
}
