/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class AssignmentStatsTests extends AbstractWireSerializingTestCase<AssignmentStats> {

    public static AssignmentStats randomDeploymentStats() {
        List<AssignmentStats.NodeStats> nodeStatsList = new ArrayList<>();
        int numNodes = randomIntBetween(1, 4);
        for (int i = 0; i < numNodes; i++) {
            var node = DiscoveryNodeUtils.create("node_" + i);
            if (randomBoolean()) {
                nodeStatsList.add(randomNodeStats(node));
            } else {
                nodeStatsList.add(
                    AssignmentStats.NodeStats.forNotStartedState(
                        node,
                        randomFrom(RoutingState.values()),
                        randomBoolean() ? null : "a good reason"
                    )
                );
            }
        }

        nodeStatsList.sort(Comparator.comparing(n -> n.getNode().getId()));

        String deploymentId = randomAlphaOfLength(5);
        String modelId = randomBoolean() ? deploymentId : randomAlphaOfLength(5);
        return new AssignmentStats(
            deploymentId,
            modelId,
            randomBoolean() ? null : randomIntBetween(1, 8),
            randomBoolean() ? null : randomIntBetween(1, 8),
            null,
            randomBoolean() ? null : randomIntBetween(1, 10000),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomLongBetween(1, 10000000)),
            Instant.now(),
            nodeStatsList,
            randomFrom(Priority.values())
        );
    }

    public static AssignmentStats.NodeStats randomNodeStats(DiscoveryNode node) {
        var lastAccess = Instant.now();
        var inferenceCount = randomNonNegativeLong();
        Double avgInferenceTime = randomDoubleBetween(0.0, 100.0, true);
        Double avgInferenceTimeExcludingCacheHit = randomDoubleBetween(0.0, 100.0, true);
        Double avgInferenceTimeLastPeriod = randomDoubleBetween(0.0, 100.0, true);

        var noInferenceCallsOnNodeYet = randomBoolean();
        if (noInferenceCallsOnNodeYet) {
            lastAccess = null;
            inferenceCount = 0;
            avgInferenceTime = null;
            avgInferenceTimeExcludingCacheHit = null;
            avgInferenceTimeLastPeriod = null;
        }
        return AssignmentStats.NodeStats.forStartedState(
            node,
            inferenceCount,
            avgInferenceTime,
            avgInferenceTimeExcludingCacheHit,
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            randomLongBetween(0, 100),
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            lastAccess,
            Instant.now(),
            randomIntBetween(1, 16),
            randomIntBetween(1, 16),
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            avgInferenceTimeLastPeriod,
            randomLongBetween(0, 100)
        );
    }

    public void testGetOverallInferenceStats() {
        String modelId = randomAlphaOfLength(10);

        AssignmentStats existingStats = new AssignmentStats(
            modelId,
            modelId,
            randomBoolean() ? null : randomIntBetween(1, 8),
            randomBoolean() ? null : randomIntBetween(1, 8),
            null,
            randomBoolean() ? null : randomIntBetween(1, 10000),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomLongBetween(1, 1000000)),
            Instant.now(),
            List.of(
                AssignmentStats.NodeStats.forStartedState(
                    DiscoveryNodeUtils.create("node_started_1"),
                    10L,
                    randomDoubleBetween(0.0, 100.0, true),
                    randomDoubleBetween(0.0, 100.0, true),
                    randomIntBetween(1, 10),
                    5,
                    4L,
                    12,
                    3,
                    Instant.now(),
                    Instant.now(),
                    randomIntBetween(1, 2),
                    randomIntBetween(1, 2),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    null,
                    1L
                ),
                AssignmentStats.NodeStats.forStartedState(
                    DiscoveryNodeUtils.create("node_started_2"),
                    12L,
                    randomDoubleBetween(0.0, 100.0, true),
                    randomDoubleBetween(0.0, 100.0, true),
                    randomIntBetween(1, 10),
                    15,
                    3L,
                    4,
                    2,
                    Instant.now(),
                    Instant.now(),
                    randomIntBetween(1, 2),
                    randomIntBetween(1, 2),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    null,
                    1L
                ),
                AssignmentStats.NodeStats.forNotStartedState(
                    DiscoveryNodeUtils.create("node_not_started_3"),
                    randomFrom(RoutingState.values()),
                    randomBoolean() ? null : "a good reason"
                )
            ),
            randomFrom(Priority.values())
        );
        InferenceStats stats = existingStats.getOverallInferenceStats();
        assertThat(stats.getModelId(), equalTo(modelId));
        assertThat(stats.getInferenceCount(), equalTo(22L));
        assertThat(stats.getFailureCount(), equalTo(41L));
    }

    public void testGetOverallInferenceStatsWithNoNodes() {
        String modelId = randomAlphaOfLength(10);

        AssignmentStats existingStats = new AssignmentStats(
            modelId,
            modelId,
            randomBoolean() ? null : randomIntBetween(1, 8),
            randomBoolean() ? null : randomIntBetween(1, 8),
            null,
            randomBoolean() ? null : randomIntBetween(1, 10000),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomLongBetween(1, 1000000)),
            Instant.now(),
            List.of(),
            randomFrom(Priority.values())
        );
        InferenceStats stats = existingStats.getOverallInferenceStats();
        assertThat(stats.getModelId(), equalTo(modelId));
        assertThat(stats.getInferenceCount(), equalTo(0L));
        assertThat(stats.getFailureCount(), equalTo(0L));
    }

    public void testGetOverallInferenceStatsWithOnlyStoppedNodes() {
        String modelId = randomAlphaOfLength(10);
        String deploymentId = randomAlphaOfLength(10);

        AssignmentStats existingStats = new AssignmentStats(
            deploymentId,
            modelId,
            randomBoolean() ? null : randomIntBetween(1, 8),
            randomBoolean() ? null : randomIntBetween(1, 8),
            null,
            randomBoolean() ? null : randomIntBetween(1, 10000),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomLongBetween(1, 1000000)),
            Instant.now(),
            List.of(
                AssignmentStats.NodeStats.forNotStartedState(
                    DiscoveryNodeUtils.create("node_not_started_1"),
                    randomFrom(RoutingState.values()),
                    randomBoolean() ? null : "a good reason"
                ),
                AssignmentStats.NodeStats.forNotStartedState(
                    DiscoveryNodeUtils.create("node_not_started_2"),
                    randomFrom(RoutingState.values()),
                    randomBoolean() ? null : "a good reason"
                )
            ),
            randomFrom(Priority.values())
        );
        InferenceStats stats = existingStats.getOverallInferenceStats();
        assertThat(stats.getModelId(), equalTo(modelId));
        assertThat(stats.getInferenceCount(), equalTo(0L));
        assertThat(stats.getFailureCount(), equalTo(0L));
    }

    @Override
    protected Writeable.Reader<AssignmentStats> instanceReader() {
        return AssignmentStats::new;
    }

    @Override
    protected AssignmentStats createTestInstance() {
        return randomDeploymentStats();
    }

    @Override
    protected AssignmentStats mutateInstance(AssignmentStats instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
