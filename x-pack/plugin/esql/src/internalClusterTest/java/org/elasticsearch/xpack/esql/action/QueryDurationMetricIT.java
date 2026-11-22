/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager.OUTCOME;
import static org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager.SOURCE_COMMAND;
import static org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager.TIME_RANGE_FILTER_FROM;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Each ES|QL query is counted once on the coordinator, labelled with its source command, the bucketed age of the lower bound of the
 * request filter's {@code @timestamp} range and its outcome, whichever node coordinates it and however it is run. Only successful queries also
 * record their duration.
 */
public class QueryDurationMetricIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestTelemetryPlugin.class);
    }

    @Before
    public void createIndices() {
        assertAcked(prepareCreate("logs").setMapping("@timestamp", "type=date", "message", "type=keyword"));
        client().prepareIndex("logs").setSource("@timestamp", "2026-01-01T00:00:00Z", "message", "hello").get();
        refresh("logs");
        assertAcked(
            prepareCreate("metrics").setSettings(Settings.builder().put("mode", "time_series").putList("routing_path", "host"))
                .setMapping("@timestamp", "type=date", "host", "type=keyword,time_series_dimension=true", "cpu", "type=long")
        );
        ensureGreen("logs", "metrics");
        telemetryPlugins().forEach(TestTelemetryPlugin::resetMeter);
    }

    public void testSuccess() {
        // Kibana's nested filter shape. The age is relative to the start derived from the took, which can be a millisecond off the
        // now that date math resolves against, so the range keeps clear of a bucket threshold.
        var filter = new BoolQueryBuilder().filter(new RangeQueryBuilder("@timestamp").gte("now-30m").lte("now"));
        run(syncEsqlQueryRequest("TS metrics | LIMIT 1").filter(filter)).close();
        assertRecorded("TS", "1_hour", "success");

        run(syncEsqlQueryRequest("FROM logs | LIMIT 1")).close();
        assertRecorded("FROM", null, "success");
    }

    public void testFailure() {
        // A short range that is long past reads old data.
        var filter = new RangeQueryBuilder("@timestamp").gte("2026-01-01T00:00:00Z").lte("2026-01-01T00:15:00Z");
        expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM logs | EVAL value = COUNT(*)").filter(filter)));
        assertRecorded("FROM", "older_than_14_days", "failed");

        // Execute directly rather than via run(), which may parse client-side to wrap as a PreparedEsqlQueryRequest.
        expectThrows(
            Exception.class,
            () -> client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest("FROM logs | NOT_A_COMMAND")).actionGet()
        );
        assertRecorded("UNKNOWN", null, "failed");
    }

    public void testStreaming() throws Exception {
        var filter = new RangeQueryBuilder("@timestamp").gte("now-2d").lte("now");
        var subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        StreamQueryTestUtils.executeStreamRequest(client(), syncEsqlQueryRequest("FROM logs | LIMIT 1").filter(filter), subscriber);
        // The stream can complete for the client before the coordinator has finished the query.
        assertBusy(() -> assertRecorded("FROM", "3_days", "success"));
    }

    /**
     * Asserts that exactly one query was counted, across all nodes, with these attributes, that its duration was recorded only if it
     * succeeded, and resets the meters.
     */
    private void assertRecorded(String sourceCommand, @Nullable String timeRangeFilterFrom, String outcome) {
        Map<String, Object> attributes = timeRangeFilterFrom == null
            ? Map.of(SOURCE_COMMAND, sourceCommand)
            : Map.of(SOURCE_COMMAND, sourceCommand, TIME_RANGE_FILTER_FROM, timeRangeFilterFrom);
        List<Measurement> counts = measurements(p -> p.getLongCounterMeasurement(PlanTelemetryManager.QUERY_TOTAL));
        assertThat(counts, hasSize(1));
        assertThat(counts.getFirst().attributes(), equalTo(Maps.copyMapWithAddedEntry(attributes, OUTCOME, outcome)));

        List<Measurement> durations = measurements(p -> p.getLongHistogramMeasurement(PlanTelemetryManager.QUERY_DURATION_HISTOGRAM));
        if (outcome.equals("success")) {
            assertThat(durations, hasSize(1));
            assertThat(durations.getFirst().attributes(), equalTo(attributes));
        } else {
            assertThat(durations, empty());
        }
        telemetryPlugins().forEach(TestTelemetryPlugin::resetMeter);
    }

    private List<Measurement> measurements(Function<TestTelemetryPlugin, List<Measurement>> instrument) {
        return telemetryPlugins().stream().flatMap(p -> instrument.apply(p).stream()).toList();
    }

    private List<TestTelemetryPlugin> telemetryPlugins() {
        return StreamSupport.stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
            .flatMap(ps -> ps.filterPlugins(TestTelemetryPlugin.class))
            .toList();
    }
}
