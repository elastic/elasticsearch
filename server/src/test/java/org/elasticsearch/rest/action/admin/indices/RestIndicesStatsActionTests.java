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

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.client.node.NodeClient;
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
            () -> action.prepareRequest(request, mock(NodeClient.class)));
        assertThat(e, hasToString(containsString("request [/_stats] contains unrecognized metric: [" + metric + "]")));
    }

    public void testUnrecognizedMetricDidYouMean() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "request_cache,fieldata,unrecognized");
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class)));
        assertThat(
            e,
            hasToString(
                containsString(
                    "request [/_stats] contains unrecognized metrics: [fieldata] -> did you mean [fielddata]?, [unrecognized]")));
    }

    public void testAllRequestWithOtherMetrics() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        final String metric = randomSubsetOf(1, RestIndicesStatsAction.METRICS.keySet()).get(0);
        params.put("metric", "_all," + metric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class)));
        assertThat(e, hasToString(containsString("request [/_stats] contains _all and individual metrics [_all," + metric + "]")));
    }

}
