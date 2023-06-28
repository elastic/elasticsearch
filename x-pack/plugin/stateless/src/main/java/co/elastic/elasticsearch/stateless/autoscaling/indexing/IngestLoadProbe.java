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

import org.elasticsearch.core.TimeValue;

import java.util.function.Function;

/**
 * This class computes the current node indexing load
 */
public class IngestLoadProbe {

    // TODO: ES-6250 makes this a cluster setting
    /**
     * MAX_TIME_TO_CLEAR_QUEUE is a threshold that defines the length of time that the current number of threads could take to clear up
     * the queued work. In other words, the amount of work that is considered manageable using the current number of threads.
     * For example, MAX_TIME_TO_CLEAR_QUEUE = 30sec means, 30 seconds worth of tasks in the queue is considered
     * manageable with the current number of threads available.
     */
    public static final TimeValue MAX_TIME_TO_CLEAR_QUEUE = TimeValue.timeValueSeconds(30);

    private final Function<String, ExecutorStats> executorStatsProvider;

    public IngestLoadProbe(Function<String, ExecutorStats> executorStatsProvider) {
        this.executorStatsProvider = executorStatsProvider;
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
                MAX_TIME_TO_CLEAR_QUEUE
            );
        }
        return totalIngestionLoad;
    }

    static double calculateIngestionLoadForExecutor(
        double averageWriteLoad,
        double averageTaskExecutionTime,
        long currentQueueSize,
        TimeValue maxTimeToClearQueue
    ) {
        if (averageTaskExecutionTime == 0.0) {
            return averageWriteLoad;
        }
        double tasksManageablePerThreadWithinMaxTime = maxTimeToClearQueue.nanos() / averageTaskExecutionTime;
        double totalThreadsNeeded = currentQueueSize / tasksManageablePerThreadWithinMaxTime;
        assert totalThreadsNeeded >= 0.0;
        return Math.max(totalThreadsNeeded, averageWriteLoad);
    }
}
