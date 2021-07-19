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

import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GetDeploymentStatsActionResponseTests extends AbstractWireSerializingTestCase<GetDeploymentStatsAction.Response> {
    @Override
    protected Writeable.Reader<GetDeploymentStatsAction.Response> instanceReader() {
        return GetDeploymentStatsAction.Response::new;
    }

    @Override
    protected GetDeploymentStatsAction.Response createTestInstance() {
        int numStats = randomIntBetween(0, 2);
        var stats = new ArrayList<GetDeploymentStatsAction.Response.AllocationStats>(numStats);
        for (var i=0; i<numStats; i++) {
            stats.add(randomDeploymentStats());
        }
        return new GetDeploymentStatsAction.Response(Collections.emptyList(), Collections.emptyList(), stats, stats.size());
    }

    private GetDeploymentStatsAction.Response.AllocationStats randomDeploymentStats() {
        List<GetDeploymentStatsAction.Response.AllocationStats.NodeStats> nodeStatsList = new ArrayList<>();
        int numNodes = randomIntBetween(1, 4);
        for (int i = 0; i < numNodes; i++) {
            nodeStatsList.add(new GetDeploymentStatsAction.Response.AllocationStats.NodeStats(
                new DiscoveryNode("node_" + i, new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT),
                randomNonNegativeLong(),
                randomDoubleBetween(0.0, 100.0, true),
                Instant.now()
            ));
        }

        return new GetDeploymentStatsAction.Response.AllocationStats(
            randomAlphaOfLength(5),
            ByteSizeValue.ofBytes(randomNonNegativeLong()),
            nodeStatsList);
    }
}
