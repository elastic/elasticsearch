/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingState;

import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class GetDeploymentStatsActionResponseTests extends AbstractWireSerializingTestCase<GetDeploymentStatsAction.Response> {
    @Override
    protected Writeable.Reader<GetDeploymentStatsAction.Response> instanceReader() {
        return GetDeploymentStatsAction.Response::new;
    }

    @Override
    protected GetDeploymentStatsAction.Response createTestInstance() {
        return createRandom();
    }

    public static GetDeploymentStatsAction.Response createRandom() {
        int numStats = randomIntBetween(0, 2);
        var stats = new ArrayList<GetDeploymentStatsAction.Response.AllocationStats>(numStats);
        for (var i = 0; i < numStats; i++) {
            stats.add(randomDeploymentStats());
        }
        stats.sort(Comparator.comparing(GetDeploymentStatsAction.Response.AllocationStats::getModelId));
        return new GetDeploymentStatsAction.Response(Collections.emptyList(), Collections.emptyList(), stats, stats.size());
    }

    private static GetDeploymentStatsAction.Response.AllocationStats randomDeploymentStats() {
        List<GetDeploymentStatsAction.Response.AllocationStats.NodeStats> nodeStatsList = new ArrayList<>();
        int numNodes = randomIntBetween(1, 4);
        for (int i = 0; i < numNodes; i++) {
            var node = new DiscoveryNode("node_" + i, new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT);
            if (randomBoolean()) {
                nodeStatsList.add(
                    GetDeploymentStatsAction.Response.AllocationStats.NodeStats.forStartedState(
                        node,
                        randomNonNegativeLong(),
                        randomBoolean() ? randomDoubleBetween(0.0, 100.0, true) : null,
                        randomIntBetween(0, 100),
                        Instant.now(),
                        Instant.now()
                    )
                );
            } else {
                nodeStatsList.add(
                    GetDeploymentStatsAction.Response.AllocationStats.NodeStats.forNotStartedState(
                        node,
                        randomFrom(RoutingState.values()),
                        randomBoolean() ? null : "a good reason"
                    )
                );
            }
        }

        nodeStatsList.sort(Comparator.comparing(n -> n.getNode().getId()));

        return new GetDeploymentStatsAction.Response.AllocationStats(
            randomAlphaOfLength(5),
            ByteSizeValue.ofBytes(randomNonNegativeLong()),
            randomBoolean() ? null : randomIntBetween(1, 8),
            randomBoolean() ? null : randomIntBetween(1, 8),
            randomBoolean() ? null : randomIntBetween(1, 10000),
            Instant.now(),
            nodeStatsList
        );
    }
}
