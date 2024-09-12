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

package co.elastic.elasticsearch.stateless.autoscaling.search.load;

import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;

import java.util.function.Function;

import static co.elastic.elasticsearch.stateless.Stateless.SHARD_READ_THREAD_POOL;
import static co.elastic.elasticsearch.stateless.autoscaling.search.load.AverageSearchLoadSampler.SEARCH_EXECUTOR;

/**
 * This class computes the current node search load
 */
public class SearchLoadProbe {

    public static final TimeValue DEFAULT_MAX_TIME_TO_CLEAR_QUEUE = TimeValue.timeValueSeconds(30);

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

    private final Function<String, ExecutorLoadStats> searchExecutorStatsProvider;
    private volatile TimeValue maxTimeToClearQueue;
    private volatile float maxQueueContributionFactor;
    private volatile double shardReadLoadThreshold;

    @SuppressWarnings("this-escape")
    public SearchLoadProbe(ClusterSettings clusterSettings, Function<String, ExecutorLoadStats> searchExecutorStatsProvider) {
        this.searchExecutorStatsProvider = searchExecutorStatsProvider;
        clusterSettings.initializeAndWatch(MAX_TIME_TO_CLEAR_QUEUE, this::setMaxTimeToClearQueue);
        clusterSettings.initializeAndWatch(MAX_QUEUE_CONTRIBUTION_FACTOR, this::setMaxQueueContributionFactor);
        clusterSettings.initializeAndWatch(SHARD_READ_LOAD_THRESHOLD_SETTING, this::setShardReadLoadThreshold);
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
        return getExecutorLoad(SEARCH_EXECUTOR);
    }

    private double getExecutorLoad(String executorName) {
        var searchExecutorStats = searchExecutorStatsProvider.apply(executorName);
        return calculateSearchLoadForExecutor(
            searchExecutorStats.threadsUsed(),
            searchExecutorStats.averageTaskExecutionEWMA(),
            searchExecutorStats.currentQueueSize(),
            maxTimeToClearQueue,
            maxQueueContributionFactor * searchExecutorStats.maxThreads(),
            searchExecutorStats.numProcessors(),
            searchExecutorStats.maxThreads()
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
     * @param maxThreadsToHandleQueue  The maximum number of threads that can be requested to handled queued work.
     * @return The number of threads needed to cope with the current search workload on this executor.
     */
    static double calculateSearchLoadForExecutor(
        double threadsUsed,
        double averageTaskExecutionTime,
        long currentQueueSize,
        TimeValue maxTimeToClearQueue,
        double maxThreadsToHandleQueue,
        double numProcessors,
        int threadPoolSize
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
}
