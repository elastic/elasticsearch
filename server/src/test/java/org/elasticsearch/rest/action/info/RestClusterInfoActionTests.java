/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.info;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.http.HttpRouteStats;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.http.HttpStatsTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.rest.action.info.RestClusterInfoAction.AVAILABLE_TARGET_NAMES;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.mockito.Mockito.mock;

public class RestClusterInfoActionTests extends ESTestCase {

    private RestClusterInfoAction action;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        action = new RestClusterInfoAction(() -> ProjectId.DEFAULT);
    }

    public void testUnrecognizediTarget() {
        var target = randomAlphaOfLength(32);
        var request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_info/").withParams(Map.of("target", target)).build();

        var e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, mock(NodeClient.class)));
        assertThat(e, hasToString(containsString("request [/_info/] contains unrecognized target: [" + target + "]")));
    }

    public void testShouldNotMixAllWithOtherTargets() {
        var invalidTarget = randomAlphaOfLength(32);
        var request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_info/")
            .withParams(Map.of("target", "_all," + invalidTarget))
            .build();

        var e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, mock(NodeClient.class)));
        assertThat(e, hasToString(containsString("request [/_info/] contains _all and individual target [_all," + invalidTarget + "]")));
    }

    public void testAllTargetAlone() throws IOException {
        var target = "_all";
        var request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_info/").withParams(Map.of("target", target)).build();

        action.prepareRequest(request, mock(NodeClient.class));
    }

    public void testMultiTargetRequest() throws IOException {
        var target = String.join(",", AVAILABLE_TARGET_NAMES);
        var request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_info/").withParams(Map.of("target", target)).build();

        action.prepareRequest(request, mock(NodeClient.class));
    }

    public void testHttpResponseMapper() {
        var nodeStats = IntStream.range(1, randomIntBetween(2, 20)).mapToObj(this::randomNodeStatsWithOnlyHttpStats).toList();
        var response = new NodesStatsResponse(new ClusterName("cluster-name"), nodeStats, List.of());

        var httpStats = (HttpStats) RestClusterInfoAction.RESPONSE_MAPPER.get(Metric.HTTP).apply(response);

        final Map<String, HttpRouteStats> httpRouteStatsMap = new HashMap<>();
        for (var ns : nodeStats) {
            ns.getHttp().httpRouteStats().forEach((k, v) -> httpRouteStatsMap.merge(k, v, HttpRouteStats::merge));
        }

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
                    .toList(),
                httpRouteStatsMap
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
            null,
            null,
            null
        );
    }
}
