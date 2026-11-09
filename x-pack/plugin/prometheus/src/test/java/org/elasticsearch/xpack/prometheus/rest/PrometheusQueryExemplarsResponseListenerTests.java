/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.prometheus.PrometheusPlugin;
import org.junit.BeforeClass;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PrometheusQueryExemplarsResponseListenerTests extends ESTestCase {

    @BeforeClass
    public static void checkFeatureFlag() {
        assumeTrue("requires the query_exemplars feature flag", PrometheusPlugin.QUERY_EXEMPLARS_FEATURE_FLAG.isEnabled());
    }

    /** The columns of an exemplars query with {@code include_timeseries}, see {@code ExemplarsRewriter#exemplarProjections}. */
    private static final List<ColumnInfoImpl> COLUMNS = List.of(
        col("@timestamp", "date"),
        col("metric_name", "keyword"),
        col("value", "double"),
        col("trace_id", "keyword"),
        col("span_id", "keyword"),
        col("_timeseries", "keyword"),
        col("_source", "_source")
    );

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant END = Instant.parse("2026-01-01T01:00:00Z");

    /**
     * Prometheus remote write exemplars carry their series labels in {@code labels} and their own labels, including the trace and span
     * ids that the {@code trace_id} and {@code span_id} columns resolve to, in {@code exemplar_labels}.
     */
    public void testPrometheusRemoteWriteExemplars() throws IOException {
        String timeseries = """
            {"labels":{"__name__":"http_request_duration_seconds","job":"api","instance":"localhost:9090"}}""";
        List<List<Object>> rows = List.of(
            row("2026-01-01T00:02:00Z", "http_request_duration_seconds", 0.5, "trace-2", "span-2", timeseries, """
                {"labels":{"__name__":"http_request_duration_seconds","job":"api","instance":"localhost:9090"},
                 "exemplar_labels":{"trace_id":"trace-2","span_id":"span-2","user":"bob"},"value":0.5}"""),
            row("2026-01-01T00:01:00Z", "http_request_duration_seconds", 0.25, "trace-1", "span-1", timeseries, """
                {"labels":{"__name__":"http_request_duration_seconds","job":"api","instance":"localhost:9090"},
                 "exemplar_labels":{"trace_id":"trace-1","span_id":"span-1"},"value":0.25}""")
        );

        ObjectPath path = convert(rows);
        assertThat(path.evaluate("status"), equalTo("success"));
        assertThat(path.evaluate("data"), hasSize(1));
        assertThat(
            path.evaluate("data.0.seriesLabels"),
            equalTo(Map.of("__name__", "http_request_duration_seconds", "job", "api", "instance", "localhost:9090"))
        );
        // oldest first, although the exemplars query returns them most recent first
        assertThat(path.evaluate("data.0.exemplars"), hasSize(2));
        assertThat(path.evaluate("data.0.exemplars.0.labels"), equalTo(Map.of("trace_id", "trace-1", "span_id", "span-1")));
        assertThat(path.evaluate("data.0.exemplars.0.value"), equalTo("0.25"));
        assertThat(path.evaluate("data.0.exemplars.0.timestamp"), equalTo(1767225660.0));
        assertThat(path.evaluate("data.0.exemplars.1.labels"), equalTo(Map.of("trace_id", "trace-2", "span_id", "span-2", "user", "bob")));
        assertThat(path.evaluate("data.0.exemplars.1.value"), equalTo("0.5"));
        assertThat(path.evaluate("data.0.exemplars.1.timestamp"), equalTo(1767225720.0));
    }

    /**
     * OTLP exemplars have their metric's name as the {@code metric_name} dimension, their trace and span ids as fields of their own,
     * and the attributes recorded with them in {@code filtered_attributes}.
     */
    public void testOtlpExemplars() throws IOException {
        List<List<Object>> rows = List.of(
            row("2026-01-01T00:01:00.123Z", "request.duration", 0.42, "00112233445566778899aabbccddeeff", "0011223344556677", """
                {"attributes":{"route":"/checkout"},"metric_name":"request.duration",
                 "resource":{"attributes":{"service.name":"checkout-service"}},"scope":{"name":"io.opentelemetry.http"},"unit":"s"}""", """
                {"@timestamp":"2026-01-01T00:01:00.123Z","attributes":{"route":"/checkout"},"metric_name":"request.duration",
                 "filtered_attributes":{"thread.id":42,"nested":{"key":"value"},"list":["a","b"],"empty":""},
                 "trace_id":"00112233445566778899aabbccddeeff","span_id":"0011223344556677","value":0.42}""")
        );

        ObjectPath path = convert(rows);
        assertThat(path.evaluate("data"), hasSize(1));
        assertThat(
            path.evaluate("data.0.seriesLabels"),
            equalTo(
                Map.of(
                    "__name__",
                    "request.duration",
                    "attributes.route",
                    "/checkout",
                    "resource.attributes.service.name",
                    "checkout-service",
                    "scope.name",
                    "io.opentelemetry.http",
                    "unit",
                    "s"
                )
            )
        );
        assertThat(
            path.evaluate("data.0.exemplars.0.labels"),
            equalTo(
                Map.of(
                    "trace_id",
                    "00112233445566778899aabbccddeeff",
                    "span_id",
                    "0011223344556677",
                    "thread.id",
                    "42",
                    "nested.key",
                    "value",
                    "list",
                    "[a, b]"
                )
            )
        );
        assertThat(path.evaluate("data.0.exemplars.0.value"), equalTo("0.42"));
        assertThat(path.evaluate("data.0.exemplars.0.timestamp"), equalTo(1767225660.123));
    }

    /** Exemplars are grouped by the labels of their series, in no particular order of the series. */
    public void testGroupsBySeries() throws IOException {
        String seriesA = """
            {"labels":{"__name__":"m","host":"a"}}""";
        String seriesB = """
            {"labels":{"__name__":"m","host":"b"}}""";
        List<List<Object>> rows = List.of(
            row("2026-01-01T00:04:00Z", "m", 4.0, "t4", "s4", seriesB, "{}"),
            row("2026-01-01T00:03:00Z", "m", 3.0, "t3", "s3", seriesA, "{}"),
            row("2026-01-01T00:02:00Z", "m", 2.0, "t2", "s2", seriesB, "{}"),
            row("2026-01-01T00:01:00Z", "m", 1.0, "t1", "s1", seriesA, "{}")
        );

        ObjectPath path = convert(rows);
        List<Object> data = path.evaluate("data");
        Map<String, List<String>> valuesByHost = new HashMap<>();
        for (Object series : data) {
            List<Object> exemplars = ObjectPath.evaluate(series, "exemplars");
            List<String> values = new ArrayList<>();
            for (Object exemplar : exemplars) {
                values.add(ObjectPath.evaluate(exemplar, "value"));
            }
            valuesByHost.put(ObjectPath.evaluate(series, "seriesLabels.host"), values);
        }
        assertThat(valuesByHost, equalTo(Map.of("a", List.of("1.0", "3.0"), "b", List.of("2.0", "4.0"))));
    }

    /**
     * The evaluated time range of the exemplars query extends before the requested start by the lookback of the selectors, the
     * exemplars before the start are dropped. Both bounds are inclusive.
     */
    public void testDropsExemplarsOutsideOfTimeRange() throws IOException {
        String series = """
            {"labels":{"__name__":"m"}}""";
        List<List<Object>> rows = List.of(
            row(END.toString(), "m", 3.0, "t3", "s3", series, "{}"),
            row(START.toString(), "m", 2.0, "t2", "s2", series, "{}"),
            row(START.minusSeconds(60).toString(), "m", 1.0, "t1", "s1", series, "{}")
        );

        ObjectPath path = convert(rows);
        assertThat(path.evaluate("data.0.exemplars"), hasSize(2));
        assertThat(path.evaluate("data.0.exemplars.0.value"), equalTo("2.0"));
        assertThat(path.evaluate("data.0.exemplars.1.value"), equalTo("3.0"));
    }

    /** Without trace and span ids, and without a {@code __name__} dimension, the {@code metric_name} column names the metric. */
    public void testMissingOptionalValues() throws IOException {
        List<List<Object>> rows = List.of(
            Arrays.asList(Instant.parse("2026-01-01T00:01:00Z").toEpochMilli(), "m", 1.0, null, null, "{\"host\":\"a\"}", null),
            Arrays.asList(Instant.parse("2026-01-01T00:02:00Z").toEpochMilli(), "m", 2.0, "t", "s", "{\"host\":\"a\"}", "{}")
        );

        ObjectPath path = convert(rows);
        assertThat(path.evaluate("data.0.seriesLabels"), equalTo(Map.of("__name__", "m", "host", "a")));
        assertThat(path.evaluate("data.0.exemplars.0.labels"), equalTo(Map.of()));
        assertThat(path.evaluate("data.0.exemplars.1.labels"), equalTo(Map.of("trace_id", "t", "span_id", "s")));
    }

    /** Without any exemplar data stream, the exemplars query is an empty relation without columns. */
    public void testNoColumns() throws IOException {
        try (
            XContentBuilder builder = PrometheusQueryExemplarsResponseListener.convertToPrometheusJson(
                List.of(),
                List.of(),
                ZoneOffset.UTC,
                START,
                END
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertThat(path.evaluate("status"), equalTo("success"));
            assertThat(path.evaluate("data"), empty());
        }
    }

    private static List<Object> row(
        String timestamp,
        String metricName,
        double value,
        String traceId,
        String spanId,
        String timeseries,
        String source
    ) {
        return List.of(Instant.parse(timestamp).toEpochMilli(), metricName, value, traceId, spanId, timeseries, source);
    }

    private static ObjectPath convert(List<List<Object>> rows) throws IOException {
        List<Page> pages = List.of(new Page(BlockUtils.fromList(TestBlockFactory.getNonBreakingInstance(), rows)));
        try (
            XContentBuilder builder = PrometheusQueryExemplarsResponseListener.convertToPrometheusJson(
                pages,
                COLUMNS,
                ZoneOffset.UTC,
                START,
                END
            )
        ) {
            return toObjectPath(builder);
        }
    }

    private static ColumnInfoImpl col(String name, String type) {
        return new ColumnInfoImpl(name, type, null);
    }

    private static ObjectPath toObjectPath(XContentBuilder builder) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(builder).streamInput())
        ) {
            return new ObjectPath(parser.map());
        }
    }
}
