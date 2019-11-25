/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.usage.UsageService;

import java.io.IOException;
import java.util.Collections;
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
        UsageService usageService = new UsageService();
        action = new RestNodesStatsAction(
            new RestController(Collections.emptySet(), null, null, null, usageService));
    }

    public void testUnrecognizedMetric() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        final String metric = randomAlphaOfLength(64);
        params.put("metric", metric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class)));
        assertThat(e, hasToString(containsString("request [/_nodes/stats] contains unrecognized metric: [" + metric + "]")));
    }

    public void testUnrecognizedMetricDidYouMean() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "os,transprot,unrecognized");
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class)));
        assertThat(
            e,
            hasToString(
                containsString(
                    "request [/_nodes/stats] contains unrecognized metrics: [transprot] -> did you mean [transport]?, [unrecognized]")));
    }

    public void testAllRequestWithOtherMetrics() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        final String metric = randomSubsetOf(1, RestNodesStatsAction.METRICS.keySet()).get(0);
        params.put("metric", "_all," + metric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class)));
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
            () -> action.prepareRequest(request, mock(NodeClient.class)));
        assertThat(e, hasToString(containsString("request [/_nodes/stats] contains unrecognized index metric: [" + indexMetric + "]")));
    }

    public void testUnrecognizedIndexMetricDidYouMean() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "indices");
        params.put("index_metric", "indexing,stroe,unrecognized");
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class)));
        assertThat(
            e,
            hasToString(
                containsString(
                    "request [/_nodes/stats] contains unrecognized index metrics: [stroe] -> did you mean [store]?, [unrecognized]")));
    }

    public void testIndexMetricsRequestWithoutIndicesMetric() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        final Set<String> metrics = new HashSet<>(RestNodesStatsAction.METRICS.keySet());
        metrics.remove("indices");
        params.put("metric", randomSubsetOf(1, metrics).get(0));
        final String indexMetric = randomSubsetOf(1, RestNodesStatsAction.FLAGS.keySet()).get(0);
        params.put("index_metric", indexMetric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class)));
        assertThat(
            e,
            hasToString(
                containsString("request [/_nodes/stats] contains index metrics [" + indexMetric + "] but indices stats not requested")));
    }

    public void testIndexMetricsRequestOnAllRequest() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "_all");
        final String indexMetric = randomSubsetOf(1, RestNodesStatsAction.FLAGS.keySet()).get(0);
        params.put("index_metric", indexMetric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class)));
        assertThat(
            e,
            hasToString(
                containsString("request [/_nodes/stats] contains index metrics [" + indexMetric + "] but all stats requested")));
    }

}
