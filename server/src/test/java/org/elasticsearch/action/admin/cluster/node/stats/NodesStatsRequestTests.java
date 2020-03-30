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

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;

public class NodesStatsRequestTests extends ESTestCase {

    /**
     * Make sure that we can set, serialize, and deserialize arbitrary sets
     * of metrics.
     */
    public void testAddMetrics() throws Exception {
        NodesStatsRequest request = new NodesStatsRequest(randomAlphaOfLength(8));
        request.indices(randomFrom(CommonStatsFlags.ALL));
        String[] metrics = randomSubsetOf(NodesStatsRequest.Metric.allMetrics()).toArray(String[]::new);
        request.addMetrics(metrics);
        NodesStatsRequest deserializedRequest = roundTripRequest(request);
        assertRequestsEqual(request, deserializedRequest);
    }

    /**
     * Check that we can add a metric.
     */
    public void testAddSingleMetric() throws Exception {
        NodesStatsRequest request = new NodesStatsRequest();
        request.addMetric(randomFrom(NodesStatsRequest.Metric.allMetrics()));
        NodesStatsRequest deserializedRequest = roundTripRequest(request);
        assertRequestsEqual(request, deserializedRequest);
    }

    /**
     * Check that we can remove a metric.
     */
    public void testRemoveSingleMetric() throws Exception {
        NodesStatsRequest request = new NodesStatsRequest();
        request.all();
        String metric = randomFrom(NodesStatsRequest.Metric.allMetrics());
        request.removeMetric(metric);
        NodesStatsRequest deserializedRequest = roundTripRequest(request);
        assertThat(request.requestedMetrics(), equalTo(deserializedRequest.requestedMetrics()));
        assertThat(metric, not(in(request.requestedMetrics())));
    }

    /**
     * Test that a newly constructed NodesStatsRequestObject requests only index metrics.
     */
    public void testNodesStatsRequestDefaults() {
        NodesStatsRequest defaultNodesStatsRequest = new NodesStatsRequest(randomAlphaOfLength(8));
        NodesStatsRequest constructedNodesStatsRequest = new NodesStatsRequest(randomAlphaOfLength(8));
        constructedNodesStatsRequest.clear();
        constructedNodesStatsRequest.indices(CommonStatsFlags.ALL);

        assertRequestsEqual(defaultNodesStatsRequest, constructedNodesStatsRequest);
    }

    /**
     * Test that the {@link NodesStatsRequest#all()} method enables all metrics.
     */
    public void testNodesInfoRequestAll() throws Exception {
        NodesStatsRequest request = new NodesStatsRequest("node");
        request.all();

        assertThat(request.indices().getFlags(), equalTo(CommonStatsFlags.ALL.getFlags()));
        assertThat(request.requestedMetrics(), equalTo(NodesStatsRequest.Metric.allMetrics()));
    }

    /**
     * Test that the {@link NodesStatsRequest#clear()} method removes all metrics.
     */
    public void testNodesInfoRequestClear() throws Exception {
        NodesStatsRequest request = new NodesStatsRequest("node");
        request.clear();

        assertThat(request.indices().getFlags(), equalTo(CommonStatsFlags.NONE.getFlags()));
        assertThat(request.requestedMetrics(), empty());
    }

    /**
     * Test that (for now) we can only add metrics from a set of known metrics.
     */
    public void testUnknownMetricsRejected() {
        String unknownMetric1 = "unknown_metric1";
        String unknownMetric2 = "unknown_metric2";
        Set<String> unknownMetrics = new HashSet<>();
        unknownMetrics.add(unknownMetric1);
        unknownMetrics.addAll(randomSubsetOf(NodesStatsRequest.Metric.allMetrics()));

        NodesStatsRequest request = new NodesStatsRequest();

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> request.addMetric(unknownMetric1));
        assertThat(exception.getMessage(), equalTo("Used an illegal metric: " + unknownMetric1));

        exception = expectThrows(IllegalStateException.class, () -> request.removeMetric(unknownMetric1));
        assertThat(exception.getMessage(), equalTo("Used an illegal metric: " + unknownMetric1));

        exception = expectThrows(IllegalStateException.class, () -> request.addMetrics(unknownMetrics.toArray(String[]::new)));
        assertThat(exception.getMessage(), equalTo("Used illegal metric: [" + unknownMetric1 + "]"));

        unknownMetrics.add(unknownMetric2);
        exception = expectThrows(IllegalStateException.class, () -> request.addMetrics(unknownMetrics.toArray(String[]::new)));
        assertThat(exception.getMessage(), equalTo("Used illegal metrics: [" + unknownMetric1 + ", " + unknownMetric2 + "]"));
    }

    /**
     * Serialize and deserialize a request.
     * @param request A request to serialize.
     * @return The deserialized, "round-tripped" request.
     */
    private static NodesStatsRequest roundTripRequest(NodesStatsRequest request) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new NodesStatsRequest(in);
            }
        }
    }

    private static void assertRequestsEqual(NodesStatsRequest request1, NodesStatsRequest request2) {
        assertThat(request1.indices().getFlags(), equalTo(request2.indices().getFlags()));
        assertThat(request1.requestedMetrics(), equalTo(request2.requestedMetrics()));
    }
}
