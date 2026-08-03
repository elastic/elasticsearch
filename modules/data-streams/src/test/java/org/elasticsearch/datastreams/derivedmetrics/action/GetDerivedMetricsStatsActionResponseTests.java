/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics.action;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.datastreams.derivedmetrics.action.GetDerivedMetricsStatsAction.DataStreamStats;
import org.elasticsearch.datastreams.derivedmetrics.action.GetDerivedMetricsStatsAction.DimensionStats;
import org.elasticsearch.datastreams.derivedmetrics.action.GetDerivedMetricsStatsAction.MetricStats;
import org.elasticsearch.datastreams.derivedmetrics.action.GetDerivedMetricsStatsAction.NodeResponse;
import org.elasticsearch.datastreams.derivedmetrics.action.GetDerivedMetricsStatsAction.NodeTotals;
import org.elasticsearch.datastreams.derivedmetrics.action.GetDerivedMetricsStatsAction.Refusals;
import org.elasticsearch.datastreams.derivedmetrics.action.GetDerivedMetricsStatsAction.Response;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * The reduction across nodes is where this API can quietly lie, because the counters are node-local and only some of them are additive.
 * These pin down which is which.
 */
public class GetDerivedMetricsStatsActionResponseTests extends ESTestCase {

    private static final DiscoveryNode NODE_ONE = DiscoveryNodeUtils.create("node-1");
    private static final DiscoveryNode NODE_TWO = DiscoveryNodeUtils.create("node-2");

    public void testTheWholeResponseSurvivesTheWire() throws IOException {
        Response response = new Response(
            new ClusterName("test"),
            List.of(
                new NodeResponse(NODE_ONE, totals(4, 4), List.of(dataStream("logs-my_app-default", 4, 12))),
                new NodeResponse(NODE_TWO, totals(7, 7), List.of())
            ),
            List.of()
        );

        Response copied = copyWriteable(response, new NamedWriteableRegistry(List.of()), Response::new);

        assertThat(copied, equalTo(response));
        assertThat(copied.dataStreams(), equalTo(response.dataStreams()));
        assertThat(copied.totals(), equalTo(response.totals()));
    }

    /**
     * A series lives on exactly one node and a refusal happened on exactly one node, so both add up. The dimension estimate does not: each
     * node's sketch counts the values it saw, and a service name written to three nodes is one service name. Summing would multiply the
     * answer by the fleet size, which would make the number useless precisely when it matters.
     */
    public void testSeriesAddUpAcrossNodesAndDimensionEstimatesDoNot() {
        Response response = new Response(
            new ClusterName("test"),
            List.of(
                new NodeResponse(NODE_ONE, totals(4, 2), List.of(dataStream("logs-my_app-default", 4, 12))),
                new NodeResponse(NODE_TWO, totals(6, 2), List.of(dataStream("logs-my_app-default", 6, 14)))
            ),
            List.of()
        );

        List<DataStreamStats> streams = response.dataStreams();
        assertThat(streams.size(), equalTo(1));
        DataStreamStats stream = streams.get(0);
        assertThat(stream.name(), equalTo("logs-my_app-default"));
        assertThat(stream.seriesHeld(), equalTo(10L));
        assertThat(stream.refusals().atStreamCap(), equalTo(4L));

        assertThat(stream.metrics().size(), equalTo(1));
        MetricStats metric = stream.metrics().get(0);
        assertThat(metric.seriesHeld(), equalTo(10L));
        assertThat(metric.interval(), equalTo("10s"));
        assertThat(metric.dimensions().size(), equalTo(1));
        // the larger of the two views, not their sum
        assertThat(metric.dimensions().get(0).estimatedDistinctValues(), equalTo(14L));
        assertThat(response.totals().seriesHeld(), equalTo(10L));
    }

    /** A dimension collapsed on any one node is collapsed as far as a reader is concerned: that node has stopped breaking down by it. */
    public void testCollapsingOnOneNodeIsReported() {
        Response response = new Response(
            new ClusterName("test"),
            List.of(
                new NodeResponse(NODE_ONE, totals(0, 0), List.of(withDimension(new DimensionStats("service.name", 4, false)))),
                new NodeResponse(NODE_TWO, totals(0, 0), List.of(withDimension(new DimensionStats("service.name", 1000, true))))
            ),
            List.of()
        );

        DimensionStats dimension = response.dataStreams().get(0).metrics().get(0).dimensions().get(0);
        assertTrue(dimension.collapsed());
        assertThat(dimension.estimatedDistinctValues(), equalTo(1000L));
    }

    public void testXContentNamesTheStreamTheMetricAndTheDimension() throws IOException {
        Response response = new Response(
            new ClusterName("test"),
            List.of(new NodeResponse(NODE_ONE, totals(4, 3), List.of(dataStream("logs-my_app-default", 4, 12)))),
            List.of()
        );

        String rendered = Strings.toString(response, true, false);

        assertThat(rendered, containsString("\"data_stream_count\" : 1"));
        assertThat(rendered, containsString("\"name\" : \"logs-my_app-default\""));
        assertThat(rendered, containsString("\"name\" : \"http.requests\""));
        assertThat(rendered, containsString("\"interval\" : \"10s\""));
        assertThat(rendered, containsString("\"name\" : \"service.name\""));
        assertThat(rendered, containsString("\"estimated_distinct_values\" : 12"));
        assertThat(rendered, containsString("\"collapsed\" : false"));
        assertThat(rendered, containsString("\"stream_cap\" : 3"));
    }

    private static DataStreamStats dataStream(String name, long seriesHeld, long estimatedValues) {
        return new DataStreamStats(
            name,
            seriesHeld,
            0,
            seriesHeld * 120,
            new Refusals(0, 2, 0, 0),
            List.of(
                new MetricStats(
                    "http.requests",
                    "10s",
                    seriesHeld,
                    seriesHeld * 120,
                    false,
                    List.of(new DimensionStats("service.name", estimatedValues, false))
                )
            )
        );
    }

    private static DataStreamStats withDimension(DimensionStats dimension) {
        return new DataStreamStats(
            "logs-my_app-default",
            0,
            0,
            0,
            Refusals.NONE,
            List.of(new MetricStats("http.requests", "10s", 0, 0, false, List.of(dimension)))
        );
    }

    private static NodeTotals totals(long seriesHeld, long droppedAtStreamCap) {
        return new NodeTotals(seriesHeld, 0, 0, new Refusals(0, droppedAtStreamCap, 0, 0), 10, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }
}
