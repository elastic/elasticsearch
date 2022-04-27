/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class AssignmentStatsTests extends AbstractWireSerializingTestCase<AssignmentStats> {

    public static AssignmentStats randomDeploymentStats() {
        List<AssignmentStats.NodeStats> nodeStatsList = new ArrayList<>();
        int numNodes = randomIntBetween(1, 4);
        for (int i = 0; i < numNodes; i++) {
            var node = new DiscoveryNode("node_" + i, new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT);
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

        return new AssignmentStats(
            randomAlphaOfLength(5),
            randomBoolean() ? null : randomIntBetween(1, 8),
            randomBoolean() ? null : randomIntBetween(1, 8),
            randomBoolean() ? null : randomIntBetween(1, 10000),
            Instant.now(),
            nodeStatsList
        );
    }

    public static AssignmentStats.NodeStats randomNodeStats(DiscoveryNode node) {
        var lastAccess = Instant.now();
        var inferenceCount = randomNonNegativeLong();
        Double avgInferenceTime = randomDoubleBetween(0.0, 100.0, true);
        Double avgInferenceTimeLastPeriod = randomDoubleBetween(0.0, 100.0, true);

        var noInferenceCallsOnNodeYet = randomBoolean();
        if (noInferenceCallsOnNodeYet) {
            lastAccess = null;
            inferenceCount = 0;
            avgInferenceTime = null;
            avgInferenceTimeLastPeriod = null;
        }
        return AssignmentStats.NodeStats.forStartedState(
            node,
            inferenceCount,
            avgInferenceTime,
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            lastAccess,
            Instant.now(),
            randomIntBetween(1, 16),
            randomIntBetween(1, 16),
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            avgInferenceTimeLastPeriod
        );
    }

    @Override
    protected Writeable.Reader<AssignmentStats> instanceReader() {
        return AssignmentStats::new;
    }

    @Override
    protected AssignmentStats createTestInstance() {
        return randomDeploymentStats();
    }
}
