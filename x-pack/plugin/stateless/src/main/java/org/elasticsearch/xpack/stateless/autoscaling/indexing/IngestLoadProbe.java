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

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.function.Function;

/**
 * This class computes the current node indexing load
 */
public class IngestLoadProbe {

    private static final Logger logger = LogManager.getLogger(IngestLoadProbe.class);

    /**
     * MAX_TIME_TO_CLEAR_QUEUE is a threshold that defines the length of time that the current number of threads could take to clear up
     * the queued work. In other words, the amount of work that is considered manageable using the current number of threads.
     * For example, MAX_TIME_TO_CLEAR_QUEUE = 30sec means, 30 seconds worth of tasks in the queue is considered
     * manageable with the current number of threads available.
     */
    public static final TimeValue DEFAULT_MAX_TIME_TO_CLEAR_QUEUE = TimeValue.timeValueSeconds(30);
    public static final Setting<TimeValue> MAX_TIME_TO_CLEAR_QUEUE = Setting.timeSetting(
        "serverless.autoscaling.indexing.sampler.max_time_to_clear_queue",
        DEFAULT_MAX_TIME_TO_CLEAR_QUEUE,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    /**
     * The amount that we'll allow the queue contribution to be relative to the size of the current node. Default value of 3
     * means 3*thread count, i.e., a max scale up of 4x.
     */
    public static final Setting<Float> MAX_QUEUE_CONTRIBUTION_FACTOR = Setting.floatSetting(
        "serverless.autoscaling.indexing.sampler.max_queue_contribution_factor",
        3,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    private final Function<String, ExecutorStats> executorStatsProvider;
    private volatile TimeValue maxTimeToClearQueue;
    private volatile float maxQueueContributionFactor;

    public IngestLoadProbe(ClusterSettings clusterSettings, Function<String, ExecutorStats> executorStatsProvider) {
        this.executorStatsProvider = executorStatsProvider;
        clusterSettings.initializeAndWatch(MAX_TIME_TO_CLEAR_QUEUE, this::setMaxTimeToClearQueue);
        clusterSettings.initializeAndWatch(MAX_QUEUE_CONTRIBUTION_FACTOR, this::setMaxQueueContributionFactor);
    }

    private void setMaxQueueContributionFactor(float maxQueueContributionFactor) {
        this.maxQueueContributionFactor = maxQueueContributionFactor;
    }

    /**
     * Returns the current ingestion load (number of WRITE threads needed to cope with the current ingestion workload).
     * <p>
     * The ingestion load is calculated as max(totalThreadsNeeded, averageWriteLoad) for each write threadpool.
     * totalThreadsNeeded is the number of threads need to handle the current load and based on queued tasks taking into account
     * MAX_TIME_TO_CLEAR_QUEUE.
     */
    public double getIngestionLoad() {
        double totalIngestionLoad = 0.0;
        for (String executorName : AverageWriteLoadSampler.WRITE_EXECUTORS) {
            var executorStats = executorStatsProvider.apply(executorName);
            totalIngestionLoad += calculateIngestionLoadForExecutor(
                executorStats.averageLoad(),
                executorStats.averageTaskExecutionEWMA(),
                executorStats.currentQueueSize(),
                maxTimeToClearQueue,
                maxQueueContributionFactor * executorStats.maxThreads()
            );
        }
        return totalIngestionLoad;
    }

    static double calculateIngestionLoadForExecutor(
        double averageWriteLoad,
        double averageTaskExecutionTime,
        long currentQueueSize,
        TimeValue maxTimeToClearQueue,
        double maxThreadsToHandleQueue
    ) {
        if (logger.isTraceEnabled()) {
            logger.trace(
                "averageWriteLoad: {}, averageTaskExecutionTime: {}, currentQueueSize: {}, maxTimeToClearQueue: {}, "
                    + "maxThreadsToHandleQueue: {}",
                averageWriteLoad,
                averageTaskExecutionTime,
                currentQueueSize,
                maxTimeToClearQueue,
                maxThreadsToHandleQueue
            );
        }
        assert maxThreadsToHandleQueue > 0.0;
        if (averageTaskExecutionTime == 0.0) {
            return averageWriteLoad;
        }
        double tasksManageablePerThreadWithinMaxTime = maxTimeToClearQueue.nanos() / averageTaskExecutionTime;
        double queueThreadsNeeded = Math.min(currentQueueSize / tasksManageablePerThreadWithinMaxTime, maxThreadsToHandleQueue);
        assert queueThreadsNeeded >= 0.0;
        return averageWriteLoad + queueThreadsNeeded;
    }

    public void setMaxTimeToClearQueue(TimeValue maxTimeToClearQueue) {
        this.maxTimeToClearQueue = maxTimeToClearQueue;
    }
}
