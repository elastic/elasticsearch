/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.ClusterStatsLevel;
import org.elasticsearch.action.NodeStatsLevel;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.object.HasToString.hasToString;
import static org.mockito.Mockito.mock;

public class RestNodesStatsActionTests extends ESTestCase {

    private RestNodesStatsAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestNodesStatsAction(() -> ProjectId.DEFAULT);
    }

    public void testUnrecognizedMetric() {
        final HashMap<String, String> params = new HashMap<>();
        final String metric = randomAlphaOfLength(64);
        params.put("metric", metric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("request [/_nodes/stats] contains unrecognized metric: [" + metric + "]")));
    }

    public void testUnrecognizedMetricDidYouMean() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "os,transprot,unrecognized");
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(
            e,
            hasToString(
                containsString(
                    "request [/_nodes/stats] contains unrecognized metrics: [transprot] -> did you mean [transport]?, [unrecognized]"
                )
            )
        );
    }

    public void testAllRequestWithOtherMetrics() {
        final HashMap<String, String> params = new HashMap<>();
        final String metric = randomFrom(Metric.ALL_NAMES);
        params.put("metric", "_all," + metric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("request [/_nodes/stats] contains _all and individual metrics [_all," + metric + "]")));
    }

    public void testUnrecognizedIndexMetric() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "indices");
        final String indexMetric = randomAlphaOfLength(64);
        params.put("index_metric", indexMetric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("request [/_nodes/stats] contains unrecognized index metric: [" + indexMetric + "]")));
    }

    public void testUnrecognizedIndexMetricDidYouMean() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "indices");
        params.put("index_metric", "indexing,stroe,unrecognized");
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(
            e,
            hasToString(
                containsString(
                    "request [/_nodes/stats] contains unrecognized index metrics: [stroe] -> did you mean [store]?, [unrecognized]"
                )
            )
        );
    }

    public void testIndexMetricsRequestWithoutIndicesMetric() {
        final HashMap<String, String> params = new HashMap<>();
        final Set<String> metrics = new HashSet<>(Metric.ALL_NAMES);
        metrics.remove("indices");
        params.put("metric", randomSubsetOf(1, metrics).get(0));
        final String indexMetric = randomSubsetOf(1, RestNodesStatsAction.FLAGS.keySet()).get(0);
        params.put("index_metric", indexMetric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(
            e,
            hasToString(
                containsString("request [/_nodes/stats] contains index metrics [" + indexMetric + "] but indices stats not requested")
            )
        );
    }

    public void testIndexMetricsRequestOnAllRequest() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "_all");
        final String indexMetric = randomSubsetOf(1, RestNodesStatsAction.FLAGS.keySet()).get(0);
        params.put("index_metric", indexMetric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(
            e,
            hasToString(containsString("request [/_nodes/stats] contains index metrics [" + indexMetric + "] but all stats requested"))
        );
    }

    public void testLevelValidation() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "_all");
        params.put("level", NodeStatsLevel.NODE.getLevel());

        // node is valid
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        action.prepareRequest(request, mock(NodeClient.class));

        // indices is valid
        params.put("level", NodeStatsLevel.INDICES.getLevel());
        request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        action.prepareRequest(request, mock(NodeClient.class));

        // shards is valid
        params.put("level", NodeStatsLevel.SHARDS.getLevel());
        request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        action.prepareRequest(request, mock(NodeClient.class));

        params.put("level", ClusterStatsLevel.CLUSTER.getLevel());
        final RestRequest invalidLevelRequest1 = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_stats")
            .withParams(params)
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(invalidLevelRequest1, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("level parameter must be one of [node] or [indices] or [shards] but was [cluster]")));

        params.put("level", "invalid");
        final RestRequest invalidLevelRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats")
            .withParams(params)
            .build();

        e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(invalidLevelRequest, mock(NodeClient.class)));
        assertThat(e, hasToString(containsString("level parameter must be one of [node] or [indices] or [shards] but was [invalid]")));
    }
}
