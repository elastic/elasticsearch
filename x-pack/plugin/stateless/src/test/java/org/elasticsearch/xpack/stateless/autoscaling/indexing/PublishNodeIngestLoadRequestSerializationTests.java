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

package org.elasticsearch.xpack.stateless.autoscaling.indexing;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestionLoad.ExecutorIngestionLoad;
import org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestionLoad.ExecutorStats;
import org.elasticsearch.xpack.stateless.autoscaling.indexing.IngestionLoad.NodeIngestionLoad;

import java.util.HashMap;
import java.util.Map;

public class PublishNodeIngestLoadRequestSerializationTests extends AbstractWireSerializingTestCase<PublishNodeIngestLoadRequest> {

    @Override
    protected Writeable.Reader<PublishNodeIngestLoadRequest> instanceReader() {
        return PublishNodeIngestLoadRequest::new;
    }

    @Override
    protected PublishNodeIngestLoadRequest createTestInstance() {
        return new PublishNodeIngestLoadRequest(UUIDs.randomBase64UUID(), randomIdentifier(), randomLong(), randomNodeIngestionLoad());
    }

    @Override
    protected PublishNodeIngestLoadRequest mutateInstance(PublishNodeIngestLoadRequest instance) {
        return switch (randomInt(3)) {
            case 0 -> new PublishNodeIngestLoadRequest(
                randomValueOtherThan(instance.getNodeId(), UUIDs::randomBase64UUID),
                instance.getNodeName(),
                instance.getSeqNo(),
                instance.getIngestionLoad()
            );
            case 1 -> new PublishNodeIngestLoadRequest(
                instance.getNodeId(),
                randomValueOtherThan(instance.getNodeName(), ESTestCase::randomIdentifier),
                instance.getSeqNo(),
                instance.getIngestionLoad()
            );
            case 2 -> new PublishNodeIngestLoadRequest(
                instance.getNodeId(),
                instance.getNodeName(),
                randomValueOtherThan(instance.getSeqNo(), ESTestCase::randomLong),
                instance.getIngestionLoad()
            );
            case 3 -> new PublishNodeIngestLoadRequest(
                instance.getNodeId(),
                instance.getNodeName(),
                instance.getSeqNo(),
                mutateNodeIngestionLoad(instance.getIngestionLoad())
            );
            default -> throw new IllegalStateException("Unexpected value: " + randomInt(2));
        };
    }

    private static ExecutorStats randomExecutorStats() {
        return new ExecutorStats(
            randomDoubleBetween(0, 100, true),
            randomDoubleBetween(10, 100000, true),
            randomInt(1000),
            randomDoubleBetween(0, 500, true),
            randomIntBetween(1, 64)
        );
    }

    private static ExecutorIngestionLoad randomExecutorIngestionLoad() {
        return new ExecutorIngestionLoad(randomDoubleBetween(0, 100, true), randomDoubleBetween(0, 50, true));
    }

    private static Map<String, Double> randomLastStableAvgTaskExecutionTimes() {
        if (rarely()) return Map.of();
        Map<String, Double> lastStableAvgTaskExecutionTimes = new HashMap<>();
        for (String executorName : AverageWriteLoadSampler.WRITE_EXECUTORS) {
            lastStableAvgTaskExecutionTimes.put(executorName, randomDoubleBetween(10, 100000, true));
        }
        return lastStableAvgTaskExecutionTimes;
    }

    private static NodeIngestionLoad randomNodeIngestionLoad() {
        Map<String, ExecutorStats> executorStats = new HashMap<>();
        Map<String, ExecutorIngestionLoad> executorIngestionLoads = new HashMap<>();
        for (String executorName : AverageWriteLoadSampler.WRITE_EXECUTORS) {
            executorStats.put(executorName, randomExecutorStats());
            executorIngestionLoads.put(executorName, randomExecutorIngestionLoad());
        }
        double totalIngestionLoad = randomDoubleBetween(0, 512, true);
        return new NodeIngestionLoad(executorStats, randomLastStableAvgTaskExecutionTimes(), executorIngestionLoads, totalIngestionLoad);
    }

    private NodeIngestionLoad mutateNodeIngestionLoad(NodeIngestionLoad load) {
        return switch (randomInt(3)) {
            case 0 -> {
                Map<String, ExecutorStats> newExecutorStats = new HashMap<>(load.executorStats());
                String executor = randomFrom(newExecutorStats.keySet());
                newExecutorStats.put(executor, randomValueOtherThan(load.executorStats().get(executor), () -> randomExecutorStats()));
                yield new NodeIngestionLoad(
                    newExecutorStats,
                    load.lastStableAvgTaskExecutionTimes(),
                    load.executorIngestionLoads(),
                    load.totalIngestionLoad()
                );
            }
            case 1 -> {
                var newValue = randomValueOtherThan(load.lastStableAvgTaskExecutionTimes(), () -> randomLastStableAvgTaskExecutionTimes());
                yield new NodeIngestionLoad(load.executorStats(), newValue, load.executorIngestionLoads(), load.totalIngestionLoad());
            }
            case 2 -> {
                Map<String, ExecutorIngestionLoad> newExecutorIngestionLoads = new HashMap<>(load.executorIngestionLoads());
                String executor = randomFrom(newExecutorIngestionLoads.keySet());
                newExecutorIngestionLoads.put(
                    executor,
                    randomValueOtherThan(load.executorIngestionLoads().get(executor), () -> randomExecutorIngestionLoad())
                );
                yield new NodeIngestionLoad(
                    load.executorStats(),
                    load.lastStableAvgTaskExecutionTimes(),
                    newExecutorIngestionLoads,
                    load.totalIngestionLoad()
                );
            }
            case 3 -> new NodeIngestionLoad(
                load.executorStats(),
                load.lastStableAvgTaskExecutionTimes(),
                load.executorIngestionLoads(),
                randomValueOtherThan(load.totalIngestionLoad(), () -> randomDoubleBetween(0, 512, true))
            );
            default -> throw new IllegalStateException("Unexpected value: " + randomInt(2));
        };
    }

}
