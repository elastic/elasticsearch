/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.ClusterStatsLevel;
import org.elasticsearch.action.NodeStatsLevel;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.object.HasToString.hasToString;
import static org.mockito.Mockito.mock;

public class RestIndicesStatsActionTests extends ESTestCase {

    private RestIndicesStatsAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestIndicesStatsAction();
    }

    public void testUnrecognizedMetric() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        final String metric = randomAlphaOfLength(64);
        params.put("metric", metric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("request [/_stats] contains unrecognized metric: [" + metric + "]")));
    }

    public void testUnrecognizedMetricDidYouMean() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "request_cache,fieldata,unrecognized");
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(
            e,
            hasToString(
                containsString("request [/_stats] contains unrecognized metrics: [fieldata] -> did you mean [fielddata]?, [unrecognized]")
            )
        );
    }

    public void testAllRequestWithOtherMetrics() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        final String metric = randomSubsetOf(1, RestIndicesStatsAction.METRICS.keySet()).get(0);
        params.put("metric", "_all," + metric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("request [/_stats] contains _all and individual metrics [_all," + metric + "]")));
    }

    public void testLevelValidation() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        params.put("level", ClusterStatsLevel.CLUSTER.getLevel());

        // cluster is valid
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_stats").withParams(params).build();
        action.prepareRequest(request, mock(NodeClient.class));

        // indices is valid
        params.put("level", ClusterStatsLevel.INDICES.getLevel());
        request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_stats").withParams(params).build();
        action.prepareRequest(request, mock(NodeClient.class));

        // shards is valid
        params.put("level", ClusterStatsLevel.SHARDS.getLevel());
        request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_stats").withParams(params).build();
        action.prepareRequest(request, mock(NodeClient.class));

        params.put("level", NodeStatsLevel.NODE.getLevel());
        final RestRequest invalidLevelRequest1 = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_stats")
            .withParams(params)
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(invalidLevelRequest1, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("level parameter must be one of [cluster] or [indices] or [shards] but was [node]")));

        params.put("level", "invalid");
        final RestRequest invalidLevelRequest2 = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_stats")
            .withParams(params)
            .build();

        e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(invalidLevelRequest2, mock(NodeClient.class)));
        assertThat(e, hasToString(containsString("level parameter must be one of [cluster] or [indices] or [shards] but was [invalid]")));
    }
}
