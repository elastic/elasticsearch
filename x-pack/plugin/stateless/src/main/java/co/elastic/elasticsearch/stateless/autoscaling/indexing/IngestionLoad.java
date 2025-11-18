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

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;

public class IngestionLoad {

    public static final TransportVersion PUBLISH_DETAILED_INGESTION_LOAD = TransportVersion.fromName("publish_detailed_ingestion_load");

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
    ) implements Writeable {

        public static NodeIngestionLoad EMPTY = new NodeIngestionLoad(Map.of(), Map.of(), 0.0);

        public static NodeIngestionLoad from(StreamInput in) throws IOException {
            final double totalIngestionLoad = in.readDouble();
            Map<String, ExecutorStats> executorStats = Map.of();
            Map<String, ExecutorIngestionLoad> executorIngestionLoads = Map.of();
            if (in.getTransportVersion().supports(PUBLISH_DETAILED_INGESTION_LOAD)) {
                executorStats = in.readImmutableMap(ExecutorStats::from);
                executorIngestionLoads = in.readImmutableMap(ExecutorIngestionLoad::from);
            }
            return new NodeIngestionLoad(executorStats, executorIngestionLoads, totalIngestionLoad);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(totalIngestionLoad);
            if (out.getTransportVersion().supports(PUBLISH_DETAILED_INGESTION_LOAD)) {
                out.writeMap(executorStats, (o, stats) -> stats.writeTo(o));
                out.writeMap(executorIngestionLoads, (o, load) -> load.writeTo(o));
            }
        }
    }

    // Detailed stats about a write threadpool
    public record ExecutorStats(
        double averageLoad,
        double averageTaskExecutionEWMA,
        int currentQueueSize,
        double averageQueueSize,
        int maxThreads
    ) implements Writeable {

        public static ExecutorStats from(StreamInput in) throws IOException {
            return new ExecutorStats(in.readDouble(), in.readDouble(), in.readVInt(), in.readDouble(), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(averageLoad);
            out.writeDouble(averageTaskExecutionEWMA);
            out.writeVInt(currentQueueSize);
            out.writeDouble(averageQueueSize);
            out.writeVInt(maxThreads);
        }
    }

    public record ExecutorIngestionLoad(double averageWriteLoad, double queueThreadsNeeded) implements Writeable {

        public static ExecutorIngestionLoad from(StreamInput in) throws IOException {
            return new ExecutorIngestionLoad(in.readDouble(), in.readDouble());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(averageWriteLoad);
            out.writeDouble(queueThreadsNeeded);
        }

        public double total() {
            return averageWriteLoad + queueThreadsNeeded;
        }
    }
}
