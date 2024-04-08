/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Granular tests for the {@link NodesInfoRequest} class. Higher-level tests
 * can be found in {@link org.elasticsearch.rest.action.admin.cluster.RestNodesInfoActionTests}.
 */
public class NodesInfoRequestTests extends ESTestCase {

    /**
     * Make sure that we can set, serialize, and deserialize arbitrary sets
     * of metrics.
     */
    public void testAddMetricsSet() {
        final NodesInfoRequest request = new NodesInfoRequest(randomAlphaOfLength(8));
        request.clear();
        final var requestedMetrics = randomSubsetOf(NodesInfoMetrics.Metric.allMetrics());
        requestedMetrics.forEach(request::addMetric);
        assertThat(request.requestedMetrics(), equalTo(Set.copyOf(requestedMetrics)));
    }

    /**
     * Check that we can remove a metric.
     */
    public void testRemoveSingleMetric() {
        NodesInfoRequest request = new NodesInfoRequest(randomAlphaOfLength(8));
        request.all();
        String metric = randomFrom(NodesInfoMetrics.Metric.allMetrics());
        request.removeMetric(metric);

        assertThat(
            request.requestedMetrics(),
            equalTo(NodesInfoMetrics.Metric.allMetrics().stream().filter(m -> m.equals(metric) == false).collect(Collectors.toSet()))
        );
    }

    /**
     * Test that a newly constructed NodesInfoRequestObject requests all of the
     * possible metrics defined in {@link NodesInfoMetrics.Metric}.
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

        assertThat(request.requestedMetrics(), equalTo(NodesInfoMetrics.Metric.allMetrics()));
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
        unknownMetrics.addAll(randomSubsetOf(NodesInfoMetrics.Metric.allMetrics()));

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
}
