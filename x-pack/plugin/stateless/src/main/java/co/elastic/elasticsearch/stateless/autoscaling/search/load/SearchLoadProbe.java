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

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;

import java.util.function.Function;

/**
 * This class computes the current node search load
 */
public class SearchLoadProbe {

    /**
     * MAX_TIME_TO_CLEAR_QUEUE is a threshold that defines the length of time that the current number of threads could take to clear up the
     * queued work. In other words, the amount of work that is considered manageable using the current number of threads. For example,
     * MAX_TIME_TO_CLEAR_QUEUE = 30sec means, 30 seconds worth of tasks in the queue is considered manageable with the current number of
     * threads available.
     */
    public static final TimeValue DEFAULT_MAX_TIME_TO_CLEAR_QUEUE = TimeValue.timeValueSeconds(1); // TODO Need to find a sensible default.
    public static final Setting<TimeValue> MAX_TIME_TO_CLEAR_QUEUE = Setting.timeSetting(
        "serverless.autoscaling.search.sampler.max_time_to_clear_queue",
        DEFAULT_MAX_TIME_TO_CLEAR_QUEUE,
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

    private final Function<String, SearchExecutorStats> searchExecutorStatsProvider;
    private volatile TimeValue maxTimeToClearQueue;
    private volatile float maxQueueContributionFactor;

    public SearchLoadProbe(ClusterSettings clusterSettings, Function<String, SearchExecutorStats> searchExecutorStatsProvider) {
        this.searchExecutorStatsProvider = searchExecutorStatsProvider;
        clusterSettings.initializeAndWatch(MAX_TIME_TO_CLEAR_QUEUE, this::setMaxTimeToClearQueue);
        clusterSettings.initializeAndWatch(MAX_QUEUE_CONTRIBUTION_FACTOR, this::setMaxQueueContributionFactor);
    }

    private void setMaxQueueContributionFactor(float maxQueueContributionFactor) {
        this.maxQueueContributionFactor = maxQueueContributionFactor;
    }

    /**
     * Returns the current search load (number of SEARCH threads needed to cope with the current search workload).
     */
    public double getSearchLoad() {
        double totalSearchLoad = 0.0;
        for (String executorName : AverageSearchLoadSampler.SEARCH_EXECUTORS) {
            var searchExecutorStats = searchExecutorStatsProvider.apply(executorName);
            totalSearchLoad += calculateSearchLoadForExecutor(
                searchExecutorStats.averageLoad(),
                searchExecutorStats.averageTaskExecutionEWMA(),
                searchExecutorStats.currentQueueSize(),
                maxTimeToClearQueue,
                maxQueueContributionFactor * searchExecutorStats.maxThreads()
            );
        }
        return totalSearchLoad;
    }

    /**
     * Calculates the "Search Load", defined as the EWMA number of threads needed to cope with the work on this executor, both running and
     * queued. This is calculated as: (number of threads needed to clear the queue within the MAX_TIME_TO_CLEAR_QUEUE setting) + (average
     * number of threads running per executor)` for each search executor.
     *
     * @param averageSearchLoad        The EWMA number of threads busy within the past 1 minute.
     * @param averageTaskExecutionTime The EWMA time/thread reported by the executor.
     * @param currentQueueSize         The queue size sampled at the current instant.
     * @param maxTimeToClearQueue      A setting that indicates the acceptable max time to clear the queue.
     * @param maxThreadsToHandleQueue  The maximum number of threads that can be requested to handled queued work.
     * @return The number of threads needed to cope with the current search workload on this executor.
     */
    static double calculateSearchLoadForExecutor(
        double averageSearchLoad,
        double averageTaskExecutionTime,
        long currentQueueSize,
        TimeValue maxTimeToClearQueue,
        double maxThreadsToHandleQueue
    ) {
        assert maxThreadsToHandleQueue > 0.0;

        var maxTimeToClearQueueNanos = maxTimeToClearQueue.nanos();

        // Protect from divide by zero errors.
        if (averageTaskExecutionTime == 0.0 || maxTimeToClearQueueNanos == 0L) {
            return averageSearchLoad;
        }

        // Determine the total threads needed to clear the queue in an acceptable timeframe,
        // given the speed at which threads are currently working, limited by the maxThreadsToHandleQueue.
        double tasksManageablePerThreadWithinMaxTime = maxTimeToClearQueueNanos / averageTaskExecutionTime;
        double queueThreadsNeeded = Math.min(currentQueueSize / tasksManageablePerThreadWithinMaxTime, maxThreadsToHandleQueue);
        assert queueThreadsNeeded >= 0.0;

        return averageSearchLoad + queueThreadsNeeded;
    }

    public void setMaxTimeToClearQueue(TimeValue maxTimeToClearQueue) {
        this.maxTimeToClearQueue = maxTimeToClearQueue;
    }
}
