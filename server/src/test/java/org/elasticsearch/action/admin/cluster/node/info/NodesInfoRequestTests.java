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

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Granular tests for the {@link NodesInfoRequest} class. Higher-level tests
 * can be found in {@link org.elasticsearch.rest.action.admin.cluster.RestNodesInfoActionTests}.
 */
public class NodesInfoRequestTests extends ESTestCase {

    /**
     * Make sure that we can set, serialize, and deserialize arbitrary sets
     * of metrics.
     */
    public void testMetricsSetters() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest(randomAlphaOfLength(8));
        request.addMetrics(randomSubsetOf(NodesInfoRequest.Metric.allMetrics()));
        NodesInfoRequest deserializedRequest = roundTripRequest(request);
        assertThat(request.requestedMetrics(), equalTo(deserializedRequest.requestedMetrics()));
    }

    /**
     * Test that a newly constructed NodesInfoRequestObject requests all of the
     * possible metrics defined in {@link NodesInfoRequest.Metric}.
     */
    public void testNodesInfoRequestDefaults() {
        NodesInfoRequest defaultNodesInfoRequest = new NodesInfoRequest(randomAlphaOfLength(8));
        NodesInfoRequest allMetricsNodesInfoRequest = new NodesInfoRequest(randomAlphaOfLength(8));
        allMetricsNodesInfoRequest.all();

        assertThat(defaultNodesInfoRequest.requestedMetrics(), equalTo(allMetricsNodesInfoRequest.requestedMetrics()));
    }

    /**
     * Test that the {@link NodesInfoRequest#all()} method sets all of the
     * metrics to {@code true}.
     */
    public void testNodesInfoRequestAll() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.all();

        assertThat(request.requestedMetrics(), equalTo(NodesInfoRequest.Metric.allMetrics()));
    }

    /**
     * Test that the {@link NodesInfoRequest#clear()} method sets all of the
     * metrics to {@code false}.
     */
    public void testNodesInfoRequestClear() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.clear();

        assertThat(request.requestedMetrics(), empty());
    }

    /**
     * Test that (for now) we can only add metrics from a set of known metrics.
     */
    public void testUnknownMetricsRejected() {
        String unknownMetric = "unknown_metric1";
        Set<String> unknownMetrics = new HashSet<>();
        unknownMetrics.add(unknownMetric);
        unknownMetrics.addAll(randomSubsetOf(NodesInfoRequest.Metric.allMetrics()));

        NodesInfoRequest request = new NodesInfoRequest();

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> request.addMetric(unknownMetric));
        assertThat(exception.getMessage(), equalTo("Used an illegal metric: " + unknownMetric));

        exception = expectThrows(IllegalStateException.class, () -> request.removeMetric(unknownMetric));
        assertThat(exception.getMessage(), equalTo("Used an illegal metric: " + unknownMetric));

        exception = expectThrows(IllegalStateException.class, () -> request.addMetrics(unknownMetrics));
        assertThat(exception.getMessage(), startsWith("Used an illegal metric: "));
    }

    /**
     * Serialize and deserialize a request.
     * @param request A request to serialize.
     * @return The deserialized, "round-tripped" request.
     */
    private static NodesInfoRequest roundTripRequest(NodesInfoRequest request) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new NodesInfoRequest(in);
            }
        }
    }
}
