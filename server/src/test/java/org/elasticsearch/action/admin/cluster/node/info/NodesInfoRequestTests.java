/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;

/**
 * Granular tests for the {@link NodesInfoRequest} class. Higher-level tests
 * can be found in {@link org.elasticsearch.rest.action.admin.cluster.RestNodesInfoActionTests}.
 */
public class NodesInfoRequestTests extends ESTestCase {

    /**
     * Make sure that we can set, serialize, and deserialize arbitrary sets
     * of metrics.
     */
    public void testAddMetricsSet() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest(randomAlphaOfLength(8));
        randomSubsetOf(NodesInfoRequest.Metric.allMetrics()).forEach(request::addMetric);
        NodesInfoRequest deserializedRequest = roundTripRequest(request);
        assertThat(request.requestedMetrics(), equalTo(deserializedRequest.requestedMetrics()));
    }

    /**
     * Check that we can add a metric.
     */
    public void testAddSingleMetric() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest(randomAlphaOfLength(8));
        request.addMetric(randomFrom(NodesInfoRequest.Metric.allMetrics()));
        NodesInfoRequest deserializedRequest = roundTripRequest(request);
        assertThat(request.requestedMetrics(), equalTo(deserializedRequest.requestedMetrics()));
    }

    /**
     * Check that we can remove a metric.
     */
    public void testRemoveSingleMetric() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest(randomAlphaOfLength(8));
        request.all();
        String metric = randomFrom(NodesInfoRequest.Metric.allMetrics());
        request.removeMetric(metric);

        NodesInfoRequest deserializedRequest = roundTripRequest(request);
        assertThat(request.requestedMetrics(), equalTo(deserializedRequest.requestedMetrics()));
        assertThat(metric, not(in(request.requestedMetrics())));
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
     * Test that the {@link NodesInfoRequest#all()} method enables all metrics.
     */
    public void testNodesInfoRequestAll() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.all();

        assertThat(request.requestedMetrics(), equalTo(NodesInfoRequest.Metric.allMetrics()));
    }

    /**
     * Test that the {@link NodesInfoRequest#clear()} method disables all metrics.
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
        String unknownMetric1 = "unknown_metric1";
        String unknownMetric2 = "unknown_metric2";
        Set<String> unknownMetrics = new HashSet<>();
        unknownMetrics.add(unknownMetric1);
        unknownMetrics.addAll(randomSubsetOf(NodesInfoRequest.Metric.allMetrics()));

        NodesInfoRequest request = new NodesInfoRequest();

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
    private static NodesInfoRequest roundTripRequest(NodesInfoRequest request) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new NodesInfoRequest(in);
            }
        }
    }
}
