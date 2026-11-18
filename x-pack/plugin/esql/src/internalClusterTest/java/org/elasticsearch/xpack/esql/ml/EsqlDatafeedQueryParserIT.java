/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ml;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParserUtils;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryWithParams;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.ParsedEsqlQuery;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucket;
import org.junit.Before;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Checks on a real cluster that the bucket span ES|QL reports in {@code column_metadata} reaches the datafeed through
 * {@link ParsedEsqlQuery#timeBucket}, which is what lets datafeeds avoid a second interpretation of the plan.
 */
public class EsqlDatafeedQueryParserIT extends AbstractEsqlIntegTestCase {

    /** Built the way the service loader builds the provider for the node: from the ES|QL plugin. */
    private EsqlDatafeedQueryParser parser() {
        EsqlPlugin plugin = internalCluster().getCurrentMasterNodeInstance(PluginsService.class)
            .filterPlugins(EsqlPlugin.class)
            .findFirst()
            .orElseThrow();
        return new EsqlDatafeedQueryParser(plugin);
    }

    @Before
    public void createLogsIndex() {
        client().admin().indices().prepareCreate("logs").setMapping("@timestamp", "type=date", "v", "type=long").get();
        client().prepareIndex("logs")
            .setSource("@timestamp", "2025-01-01T10:30:00Z", "v", 1)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    private Optional<TimeBucket> timeBucket(String query, String timeColumn) {
        ParsedEsqlQuery parsed = parser().parse(query);
        // A filter that matches nothing reads no rows and, unlike LIMIT 0, keeps the column metadata.
        var request = syncEsqlQueryRequest(parsed.columnMetadataProbeQuery().query()).filter(new MatchNoneQueryBuilder());
        try (EsqlQueryResponse response = run(request)) {
            return parsed.timeBucket(response.columns(), timeColumn);
        }
    }

    public void testBucketShouldReportItsSpan() {
        assertThat(
            timeBucket("FROM logs | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)", "b"),
            equalTo(Optional.of(new TimeBucket(1, "hour")))
        );
    }

    public void testTBucketShouldReportItsSpan() {
        assertThat(
            timeBucket("FROM logs | STATS c = COUNT(*) BY b = TBUCKET(15 minutes)", "b"),
            equalTo(Optional.of(new TimeBucket(15, "minute")))
        );
        assertThat(timeBucket("FROM logs | STATS c = COUNT(*) BY b = TBUCKET(1 day)", "b"), equalTo(Optional.of(new TimeBucket(1, "day"))));
    }

    public void testSpanShouldSurviveCommandsAfterTheStats() {
        assertThat(
            timeBucket("FROM logs | STATS c = COUNT(*) BY b = TBUCKET(1 hour) | EVAL d = c + 1 | SORT b ASC | KEEP b, d | LIMIT 10", "b"),
            equalTo(Optional.of(new TimeBucket(1, "hour")))
        );
    }

    public void testDateTruncShouldReportNoSpan() {
        assertThat(timeBucket("FROM logs | STATS c = COUNT(*) BY b = DATE_TRUNC(1 hour, @timestamp)", "b"), equalTo(Optional.empty()));
    }

    public void testRawQueryShouldReportNoSpan() {
        assertThat(timeBucket("FROM logs | KEEP @timestamp, v", "@timestamp"), equalTo(Optional.empty()));
    }

    public void testTimeSeriesSourceProbeShouldNotReadStandardIndices() {
        client().admin()
            .indices()
            .prepareCreate("metrics-tsdb")
            .setSettings(
                Settings.builder()
                    .put("index.mode", "time_series")
                    .put("index.routing_path", "host")
                    .put("index.time_series.start_time", "2025-01-01T00:00:00Z")
                    .put("index.time_series.end_time", "2025-01-02T00:00:00Z")
            )
            .setMapping("@timestamp", "type=date", "host", "type=keyword,time_series_dimension=true", "cpu", "type=double")
            .get();
        client().prepareIndex("metrics-tsdb")
            .setSource("@timestamp", "2025-01-01T10:30:00Z", "host", "a", "cpu", 1.0)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        client().admin().indices().prepareCreate("metrics-standard").setMapping("@timestamp", "type=date", "cpu", "type=double").get();
        client().prepareIndex("metrics-standard")
            .setSource("@timestamp", "2025-01-01T10:30:00Z", "cpu", 2.0)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        ParsedEsqlQuery parsed = parser().parse("TS metrics-* | STATS m = max(cpu) BY b = TBUCKET(1 hour)");
        var summary = parsed.sourceRangeSummaryQuery("@timestamp");
        var request = syncEsqlQueryRequest(summary.query());
        request.params(
            new QueryParams(
                List.of(new QueryParam("timeField", "@timestamp", DataType.KEYWORD, ParserUtils.ParamClassification.IDENTIFIER))
            )
        );
        try (EsqlQueryResponse response = run(request)) {
            List<Object> row = getValuesList(response).getFirst();
            int totalHits = response.columns().stream().map(c -> c.name()).toList().indexOf(ParsedEsqlQuery.TOTAL_HITS_COLUMN);
            assertThat(row.get(totalHits), equalTo(1L));
        }
    }

    private EsqlQueryResponse run(EsqlQueryWithParams query) {
        var request = syncEsqlQueryRequest(query.query());
        request.params(
            new QueryParams(
                query.params()
                    .stream()
                    .map(p -> new QueryParam(p.name(), p.value(), DataType.KEYWORD, ParserUtils.ParamClassification.IDENTIFIER))
                    .toList()
            )
        );
        return run(request);
    }

    private void indexLogs() {
        for (String timestamp : List.of("2025-01-01T11:30:00Z", "2025-01-01T12:30:00Z")) {
            client().prepareIndex("logs").setSource("@timestamp", timestamp, "v", 2).get();
        }
        client().admin().indices().prepareRefresh("logs").get();
    }

    private static int columnIndex(EsqlQueryResponse response, String name) {
        int index = response.columns().stream().map(c -> c.name()).toList().indexOf(name);
        assertThat("column [" + name + "] in " + response.columns(), index >= 0, equalTo(true));
        return index;
    }

    /** The time zone of the query decides the offset the instant is written with, not the instant. */
    private static Instant instant(Object value) {
        return OffsetDateTime.parse(value.toString()).toInstant();
    }

    /** An accepted query and the column that holds its time. */
    private record Accepted(String query, String timeColumn) {}

    public void testAllGeneratedQueriesShouldRunForAcceptedQueries() {
        indexLogs();
        String bucket = "STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)";
        List<Accepted> accepted = List.of(
            new Accepted("FROM logs", "@timestamp"),
            new Accepted("FROM logs METADATA _index", "@timestamp"),
            new Accepted("SET time_zone=\"Europe/Berlin\"; FROM logs", "@timestamp"),
            new Accepted("FROM logs | " + bucket + " // trailing comment", "b"),
            new Accepted("FROM (FROM logs | WHERE v > 0)", "@timestamp"),
            new Accepted("FROM logs | INLINE STATS m = MAX(v) | LIMIT 10", "@timestamp"),
            new Accepted("FROM logs | " + bucket + " | LIMIT 5", "b"),
            new Accepted("FROM logs | WHERE v > 0 | EVAL w = v + 1 | KEEP @timestamp, w | SORT w", "@timestamp")
        );
        for (Accepted a : accepted) {
            ParsedEsqlQuery parsed = parser().parse(a.query());
            String context = a.query();

            try (EsqlQueryResponse response = run(parsed.limitZeroQuery())) {
                assertThat(context, getValuesList(response).size(), equalTo(0));
                assertThat(context, response.columns().isEmpty(), equalTo(false));
            }

            int rows;
            try (EsqlQueryResponse response = run(syncEsqlQueryRequest(a.query()))) {
                rows = getValuesList(response).size();
            }
            try (EsqlQueryResponse response = run(parsed.outputRowCountQuery())) {
                assertThat(context, getValuesList(response).getFirst().getFirst(), equalTo((long) rows));
            }

            try (EsqlQueryResponse response = run(parsed.orderedByTimeQuery(a.timeColumn(), OptionalLong.empty()))) {
                List<List<Object>> values = getValuesList(response);
                assertThat(context, values.size(), equalTo(rows));
                int time = columnIndex(response, a.timeColumn());
                List<String> times = values.stream().map(row -> String.valueOf(row.get(time))).toList();
                assertThat(context, times, equalTo(times.stream().sorted().toList()));
            }
            try (EsqlQueryResponse response = run(parsed.orderedByTimeQuery(a.timeColumn(), OptionalLong.of(1)))) {
                assertThat(context, getValuesList(response).size(), equalTo(1));
            }

            try (EsqlQueryResponse response = run(parsed.sourceRangeSummaryQuery("@timestamp"))) {
                List<Object> row = getValuesList(response).getFirst();
                assertThat(
                    context,
                    instant(row.get(columnIndex(response, ParsedEsqlQuery.EARLIEST_TIME_COLUMN))),
                    equalTo(Instant.parse("2025-01-01T10:30:00.000Z"))
                );
                assertThat(
                    context,
                    instant(row.get(columnIndex(response, ParsedEsqlQuery.LATEST_TIME_COLUMN))),
                    equalTo(Instant.parse("2025-01-01T12:30:00.000Z"))
                );
                assertThat(context, row.get(columnIndex(response, ParsedEsqlQuery.TOTAL_HITS_COLUMN)), equalTo(3L));
            }

            try (EsqlQueryResponse response = run(parsed.sourceTimeFieldProbeQuery("@timestamp"))) {
                assertThat(context, getValuesList(response).size(), equalTo(0));
            }
            expectThrows(Exception.class, () -> run(parsed.sourceTimeFieldProbeQuery("missing_field")).close());
        }
    }

    private void createTimeSeriesIndex() {
        client().admin()
            .indices()
            .prepareCreate("metrics-tsdb")
            .setSettings(
                Settings.builder()
                    .put("index.mode", "time_series")
                    .put("index.routing_path", "host")
                    .put("index.time_series.start_time", "2025-01-01T00:00:00Z")
                    .put("index.time_series.end_time", "2025-01-02T00:00:00Z")
            )
            .setMapping("@timestamp", "type=date", "host", "type=keyword,time_series_dimension=true", "cpu", "type=double")
            .get();
        for (int minute : new int[] { 30, 31, 32 }) {
            client().prepareIndex("metrics-tsdb")
                .setSource("@timestamp", "2025-01-01T10:" + minute + ":00Z", "host", "a", "cpu", 1.0)
                .get();
        }
        client().admin().indices().prepareRefresh("metrics-tsdb").get();
    }

    public void testTimeSeriesSourceShouldBeProbedOverTimeSeriesIndicesOnly() {
        createTimeSeriesIndex();
        // A standard index that maps the time field as keyword would make a FROM over both indices fail with a type conflict.
        client().admin().indices().prepareCreate("metrics-standard").setMapping("@timestamp", "type=keyword", "cpu", "type=double").get();
        client().prepareIndex("metrics-standard")
            .setSource("@timestamp", "not a date", "cpu", 2.0)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        ParsedEsqlQuery parsed = parser().parse("TS metrics-* | STATS m = max(cpu) BY b = TBUCKET(1 hour)");
        try (EsqlQueryResponse response = run(parsed.sourceRangeSummaryQuery("@timestamp"))) {
            List<Object> row = getValuesList(response).getFirst();
            assertThat(
                instant(row.get(columnIndex(response, ParsedEsqlQuery.EARLIEST_TIME_COLUMN))),
                equalTo(Instant.parse("2025-01-01T10:30:00.000Z"))
            );
            assertThat(
                instant(row.get(columnIndex(response, ParsedEsqlQuery.LATEST_TIME_COLUMN))),
                equalTo(Instant.parse("2025-01-01T10:32:00.000Z"))
            );
            assertThat(row.get(columnIndex(response, ParsedEsqlQuery.TOTAL_HITS_COLUMN)), equalTo(3L));
        }
        try (EsqlQueryResponse response = run(parsed.sourceTimeFieldProbeQuery("@timestamp"))) {
            assertThat(getValuesList(response).size(), equalTo(0));
        }
        expectThrows(Exception.class, () -> run(parsed.sourceTimeFieldProbeQuery("missing_field")).close());
    }

    public void testProviderShouldBeConstructibleFromThePluginTheWayThePluginsServiceDoes() throws Exception {
        EsqlPlugin plugin = internalCluster().getCurrentMasterNodeInstance(PluginsService.class)
            .filterPlugins(EsqlPlugin.class)
            .findFirst()
            .orElseThrow();
        EsqlQueryParser provider = EsqlDatafeedQueryParser.class.getConstructor(EsqlPlugin.class).newInstance(plugin);
        assertThat(provider.parse("FROM logs | STATS c = COUNT(*)").indexPatterns(), equalTo(java.util.Set.of("logs")));
        expectThrows(java.lang.reflect.InvocationTargetException.class, () -> EsqlDatafeedQueryParser.class.getConstructor().newInstance());
    }
}
