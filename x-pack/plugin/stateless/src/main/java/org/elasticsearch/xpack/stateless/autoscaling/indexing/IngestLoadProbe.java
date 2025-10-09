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
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    /**
     * Whether to include ingestion load from the write coordination executors to the reported total for autoscaling purpose.
     */
    public static final Setting<Boolean> INCLUDE_WRITE_COORDINATION_EXECUTORS_ENABLED = Setting.boolSetting(
        "serverless.autoscaling.indexing.sampler.include_write_coordination.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    /**
     * A newly started indexing node may have a large number of tasks queued up due to buffered indexing requests on the nodes that
     * are leaving the cluster and their shards are being relocated, or possibly due to a cold cache. To avoid a short spike in the
     * queue size to cause an unnecessary scale up, we ignore the queue contribution to the ingestion load for this initial interval.
     */
    public static final TimeValue DEFAULT_INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION = TimeValue.THIRTY_SECONDS;
    public static final Setting<TimeValue> INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION = Setting.timeSetting(
        "serverless.autoscaling.indexing.sampler.initial_interval_to_ignore_queue_contribution",
        DEFAULT_INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    private final Function<String, ExecutorStats> executorStatsProvider;
    private final Map<String, ExecutorIngestionLoad> ingestionLoadPerExecutor;
    private final TimeProvider timeProvider;
    private volatile TimeValue maxTimeToClearQueue;
    private volatile float maxQueueContributionFactor;
    private volatile boolean includeWriteCoordinationExecutors;
    private volatile TimeValue initialIntervalToIgnoreQueueContribution;
    private volatile long probeStartTimeInMillis;

    @SuppressWarnings("this-escape")
    public IngestLoadProbe(
        ClusterSettings clusterSettings,
        Function<String, ExecutorStats> executorStatsProvider,
        TimeProvider timeProvider
    ) {
        this.executorStatsProvider = executorStatsProvider;
        this.timeProvider = timeProvider;
        clusterSettings.initializeAndWatch(MAX_TIME_TO_CLEAR_QUEUE, this::setMaxTimeToClearQueue);
        clusterSettings.initializeAndWatch(MAX_QUEUE_CONTRIBUTION_FACTOR, this::setMaxQueueContributionFactor);
        clusterSettings.initializeAndWatch(INCLUDE_WRITE_COORDINATION_EXECUTORS_ENABLED, this::setIncludeWriteCoordinationExecutors);
        clusterSettings.initializeAndWatch(
            INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION,
            value -> this.initialIntervalToIgnoreQueueContribution = value
        );
        ingestionLoadPerExecutor = new ConcurrentHashMap<>(AverageWriteLoadSampler.WRITE_EXECUTORS.size());
        AverageWriteLoadSampler.WRITE_EXECUTORS.forEach(name -> ingestionLoadPerExecutor.put(name, new ExecutorIngestionLoad(0.0, 0.0)));
    }

    private void setMaxQueueContributionFactor(float maxQueueContributionFactor) {
        this.maxQueueContributionFactor = maxQueueContributionFactor;
    }

    private void setIncludeWriteCoordinationExecutors(boolean enabled) {
        this.includeWriteCoordinationExecutors = enabled;
    }

    /**
     * Returns the current ingestion load (number of WRITE threads needed to cope with the current ingestion workload).
     * <p>
     * The ingestion load is calculated as (averageWriteLoad + queueThreadsNeeded) for each write threadpool.
     * queueThreadsNeeded is the number of threads need to handle the current queued tasks, taking into account
     * MAX_TIME_TO_CLEAR_QUEUE.
     */
    public double getIngestionLoad() {
        long currentTimeInMillis = timeProvider.relativeTimeInMillis();
        if (probeStartTimeInMillis == 0) {
            probeStartTimeInMillis = currentTimeInMillis;
        }
        double totalIngestionLoad = 0.0;
        for (String executorName : AverageWriteLoadSampler.WRITE_EXECUTORS) {
            var executorStats = executorStatsProvider.apply(executorName);
            var ingestionLoadForExecutor = calculateIngestionLoadForExecutor(
                executorName,
                executorStats.averageLoad(),
                executorStats.averageTaskExecutionEWMA(),
                executorStats.averageQueueSize(),
                maxTimeToClearQueue,
                maxQueueContributionFactor * executorStats.maxThreads()
            );
            if (ingestionLoadForExecutor.queueThreadsNeeded > 0.0
                && currentTimeInMillis - probeStartTimeInMillis < initialIntervalToIgnoreQueueContribution.millis()) {
                // This is a newly started node as defined by the INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION
                // setting. Drop the queue contribution.
                logger.info(
                    "dropping queue contribution [{}] for executor [{}]"
                        + " since we are within the first {}s of sampling ingest load on this node",
                    ingestionLoadForExecutor,
                    executorName,
                    initialIntervalToIgnoreQueueContribution.seconds()
                );
                ingestionLoadForExecutor = new ExecutorIngestionLoad(ingestionLoadForExecutor.averageWriteLoad, 0.0);
            }
            ingestionLoadPerExecutor.put(executorName, ingestionLoadForExecutor);
            // Do not include ingestion load from write coordination executors if disabled. But they are still recorded
            // in the above ingestionLoadPerExecutor for metrics purpose.
            if (includeWriteCoordinationExecutors
                || (ThreadPool.Names.WRITE_COORDINATION.equals(executorName) == false
                    && ThreadPool.Names.SYSTEM_WRITE_COORDINATION.equals(executorName) == false)) {
                totalIngestionLoad += ingestionLoadForExecutor.total();
            }
        }
        return totalIngestionLoad;
    }

    public record ExecutorIngestionLoad(double averageWriteLoad, double queueThreadsNeeded) {
        public double total() {
            return averageWriteLoad + queueThreadsNeeded;
        }
    }

    public ExecutorIngestionLoad getExecutorIngestionLoad(String executor) {
        assert AverageWriteLoadSampler.WRITE_EXECUTORS.contains(executor);
        return ingestionLoadPerExecutor.get(executor);
    }

    static ExecutorIngestionLoad calculateIngestionLoadForExecutor(
        String executor,
        double averageWriteLoad,
        double averageTaskExecutionTime,
        double averageQueueSize,
        TimeValue maxTimeToClearQueue,
        double maxThreadsToHandleQueue
    ) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "{}: averageWriteLoad: {}, averageTaskExecutionTime: {}, averageQueueSize: {}, maxTimeToClearQueue: {}, "
                    + "maxThreadsToHandleQueue: {}",
                executor,
                averageWriteLoad,
                averageTaskExecutionTime,
                averageQueueSize,
                maxTimeToClearQueue,
                maxThreadsToHandleQueue
            );
        }
        assert maxThreadsToHandleQueue > 0.0;
        if (averageTaskExecutionTime == 0.0) {
            return new ExecutorIngestionLoad(averageWriteLoad, 0.0);
        }
        double tasksManageablePerThreadWithinMaxTime = maxTimeToClearQueue.nanos() / averageTaskExecutionTime;
        double queueThreadsNeeded = Math.min(averageQueueSize / tasksManageablePerThreadWithinMaxTime, maxThreadsToHandleQueue);
        assert queueThreadsNeeded >= 0.0;
        return new ExecutorIngestionLoad(averageWriteLoad, queueThreadsNeeded);
    }

    public void setMaxTimeToClearQueue(TimeValue maxTimeToClearQueue) {
        this.maxTimeToClearQueue = maxTimeToClearQueue;
    }
}
