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

import java.util.Map;

public class IngestionLoad {

    /**
     * {@link NodeIngestionLoad} contains {@link ExecutorStats} and {@link ExecutorIngestionLoad} for each write threadpool, and
     * the current total node ingestion load (number of WRITE threads needed to cope with the current ingestion workload).
     * <p>
     * The ingestion load is calculated as (averageWriteLoad + queueThreadsNeeded) for each write threadpool.
     * queueThreadsNeeded is the number of threads need to handle the current queued tasks, taking into account
     * MAX_TIME_TO_CLEAR_QUEUE.
     */
    public record NodeIngestionLoad(
        Map<String, ExecutorStats> executorStats,
        Map<String, ExecutorIngestionLoad> executorIngestionLoads,
        double totalIngestionLoad
    ) {}

    // Detailed stats about a write threadpool
    public record ExecutorStats(
        double averageLoad,
        double averageTaskExecutionEWMA,
        int currentQueueSize,
        double averageQueueSize,
        int maxThreads
    ) {}

    public record ExecutorIngestionLoad(double averageWriteLoad, double queueThreadsNeeded) {
        public double total() {
            return averageWriteLoad + queueThreadsNeeded;
        }
    }
}
