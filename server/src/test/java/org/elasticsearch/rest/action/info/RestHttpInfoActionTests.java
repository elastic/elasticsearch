/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.info;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.http.HttpStatsTests;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

public class RestHttpInfoActionTests extends ESTestCase {

    public void testXContentToChunks() {
        var nodeStats = IntStream.range(1, randomIntBetween(2, 20)).mapToObj(this::randomNodeStatsWithOnlyHttpStats).toList();
        var response = new NodesStatsResponse(new ClusterName("cluster-name"), nodeStats, List.of());

        var httpStats = (HttpStats) new RestHttpInfoAction().xContentChunks(response);

        assertEquals(
            httpStats,
            new HttpStats(
                nodeStats.stream().map(NodeStats::getHttp).mapToLong(HttpStats::serverOpen).sum(),
                nodeStats.stream().map(NodeStats::getHttp).mapToLong(HttpStats::totalOpen).sum(),
                nodeStats.stream()
                    .map(NodeStats::getHttp)
                    .map(HttpStats::clientStats)
                    .map(Collection::stream)
                    .reduce(Stream.of(), Stream::concat)
                    .toList()
            )
        );
    }

    private NodeStats randomNodeStatsWithOnlyHttpStats(int i) {
        return new NodeStats(
            mock(DiscoveryNode.class),
            randomLong(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            HttpStatsTests.randomHttpStats(),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }
}
