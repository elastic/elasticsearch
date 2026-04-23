/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction.Response;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStatsTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStatsTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelSizeStatsTests;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction.Response.RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats.MEMORY_STAT_TRANSPORT_VERSION;

public class GetTrainedModelsStatsActionResponseTests extends AbstractBWCWireSerializationTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return createInstance();
    }

    @Override
    protected Response mutateInstance(Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static Response createInstance() {
        int listSize = randomInt(10);
        List<Response.TrainedModelStats> trainedModelStats = Stream.generate(() -> randomAlphaOfLength(10))
            .limit(listSize)
            .map(
                id -> new Response.TrainedModelStats(
                    id,
                    randomBoolean() ? TrainedModelSizeStatsTests.createRandom() : null,
                    randomBoolean() ? randomIngestStats() : null,
                    randomIntBetween(0, 10),
                    randomBoolean() ? InferenceStatsTests.createTestInstance(id, null) : null,
                    randomBoolean() ? AssignmentStatsTests.randomDeploymentStats() : null
                )
            )
            .collect(Collectors.toList());
        return new Response(new QueryPage<>(trainedModelStats, randomLongBetween(listSize, 1000), RESULTS_FIELD));
    }

    public static IngestStats randomIngestStats() {
        List<String> pipelineIds = Stream.generate(() -> randomAlphaOfLength(10)).limit(randomIntBetween(0, 10)).toList();
        return new IngestStats(
            new IngestStats.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            pipelineIds.stream()
                .map(id -> new IngestStats.PipelineStat(ProjectId.DEFAULT, id, randomStats(), randomByteStats()))
                .collect(Collectors.toList()),
            pipelineIds.isEmpty()
                ? Map.of()
                : Map.of(
                    ProjectId.DEFAULT,
                    pipelineIds.stream().collect(Collectors.toMap(Function.identity(), v -> randomProcessorStats()))
                )
        );
    }

    private static IngestStats.Stats randomStats() {
        return new IngestStats.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    private static IngestStats.ByteStats randomByteStats() {
        return new IngestStats.ByteStats(randomNonNegativeLong(), randomNonNegativeLong());
    }

    private static List<IngestStats.ProcessorStat> randomProcessorStats() {
        return Stream.generate(() -> randomAlphaOfLength(10))
            .limit(randomIntBetween(0, 10))
            .map(name -> new IngestStats.ProcessorStat(name, "inference", randomStats()))
            .collect(Collectors.toList());
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response mutateInstanceForVersion(Response instance, TransportVersion version) {
        if (version.supports(MEMORY_STAT_TRANSPORT_VERSION) == false) {
            return new Response(
                new QueryPage<>(
                    instance.getResources()
                        .results()
                        .stream()
                        .map(
                            stats -> new Response.TrainedModelStats(
                                stats.getModelId(),
                                stats.getModelSizeStats(),
                                stats.getIngestStats(),
                                stats.getPipelineCount(),
                                stats.getInferenceStats(),
                                stats.getDeploymentStats() == null
                                    ? null
                                    : new AssignmentStats(
                                        stats.getDeploymentStats().getDeploymentId(),
                                        stats.getDeploymentStats().getModelId(),
                                        stats.getDeploymentStats().getThreadsPerAllocation(),
                                        stats.getDeploymentStats().getNumberOfAllocations(),
                                        stats.getDeploymentStats().getAdaptiveAllocationsSettings(),
                                        stats.getDeploymentStats().getQueueCapacity(),
                                        stats.getDeploymentStats().getCacheSize(),
                                        stats.getDeploymentStats().getStartTime(),
                                        stats.getDeploymentStats()
                                            .getNodeStats()
                                            .stream()
                                            .map(
                                                nodeStats -> new AssignmentStats.NodeStats(
                                                    nodeStats.getNode(),
                                                    nodeStats.getInferenceCount().orElse(null),
                                                    nodeStats.getAvgInferenceTime().orElse(null),
                                                    nodeStats.getAvgInferenceTimeExcludingCacheHit().orElse(null),
                                                    nodeStats.getLastAccess(),
                                                    nodeStats.getPendingCount(),
                                                    nodeStats.getErrorCount(),
                                                    nodeStats.getCacheHitCount().orElse(null),
                                                    nodeStats.getRejectedExecutionCount(),
                                                    nodeStats.getTimeoutCount(),
                                                    nodeStats.getRoutingState(),
                                                    nodeStats.getStartTime(),
                                                    nodeStats.getThreadsPerAllocation(),
                                                    nodeStats.getNumberOfAllocations(),
                                                    nodeStats.getPeakThroughput(),
                                                    nodeStats.getThroughputLastPeriod(),
                                                    nodeStats.getAvgInferenceTimeLastPeriod(),
                                                    nodeStats.getCacheHitCountLastPeriod().orElse(null),
                                                    null  // avgInferenceProcessMemoryRssBytes is null for old versions
                                                )
                                            )
                                            .toList(),
                                        stats.getDeploymentStats().getPriority()
                                    )
                            )
                        )
                        .toList(),
                    instance.getResources().count(),
                    RESULTS_FIELD
                )
            );
        }
        return instance;
    }
}
