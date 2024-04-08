/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
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
        String[] metrics = randomSubsetOf(NodesStatsRequestParameters.Metric.allMetrics()).toArray(String[]::new);
        request.addMetrics(metrics);
        NodesStatsRequest deserializedRequest = roundTripRequest(request);
        assertRequestsEqual(request, deserializedRequest);
    }

    /**
     * Check that we can add a metric.
     */
    public void testAddSingleMetric() throws Exception {
        NodesStatsRequest request = new NodesStatsRequest();
        request.addMetric(randomFrom(NodesStatsRequestParameters.Metric.allMetrics()));
        NodesStatsRequest deserializedRequest = roundTripRequest(request);
        assertRequestsEqual(request, deserializedRequest);
    }

    /**
     * Check that we can remove a metric.
     */
    public void testRemoveSingleMetric() throws Exception {
        NodesStatsRequest request = new NodesStatsRequest();
        request.all();
        String metric = randomFrom(NodesStatsRequestParameters.Metric.allMetrics());
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
        assertThat(request.requestedMetrics(), equalTo(NodesStatsRequestParameters.Metric.allMetrics()));
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
        unknownMetrics.addAll(randomSubsetOf(NodesStatsRequestParameters.Metric.allMetrics()));

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

    public void testGetDescription() {
        final var request = new NodesStatsRequest("nodeid1", "nodeid2");
        request.clear();
        request.addMetrics(NodesStatsRequestParameters.Metric.OS.metricName(), NodesStatsRequestParameters.Metric.TRANSPORT.metricName());
        request.indices(new CommonStatsFlags(CommonStatsFlags.Flag.Store, CommonStatsFlags.Flag.Flush));
        final var description = request.getDescription();

        assertThat(
            description,
            allOf(
                containsString("nodeid1"),
                containsString("nodeid2"),
                containsString(NodesStatsRequestParameters.Metric.OS.metricName()),
                containsString(NodesStatsRequestParameters.Metric.TRANSPORT.metricName()),
                not(containsString(NodesStatsRequestParameters.Metric.SCRIPT.metricName())),
                containsString(CommonStatsFlags.Flag.Store.toString()),
                containsString(CommonStatsFlags.Flag.Flush.toString()),
                not(containsString(CommonStatsFlags.Flag.FieldData.toString()))
            )
        );

        assertEquals(description, request.createTask(1, "", "", TaskId.EMPTY_TASK_ID, Map.of()).getDescription());
    }
}
