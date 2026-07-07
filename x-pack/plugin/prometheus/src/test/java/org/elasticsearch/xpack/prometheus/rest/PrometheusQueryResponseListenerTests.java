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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQueryResponseListener.QueryMode;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class PrometheusQueryResponseListenerTests extends ESTestCase {

    // -------------------------------------------------------------------------
    // Range query (matrix) tests
    // -------------------------------------------------------------------------

    // Column names are bare label names (e.g. "job", "instance") — no "labels." prefix.
    // The Prometheus data stream maps labels with type: passthrough, so PROMQL resolves
    // labels.job → column "job", labels.instance → column "instance", etc.
    public void testConvertRangeQueryWithIndividualLabels() throws IOException {
        List<ColumnInfoImpl> columns = List.of(
            col("value", "double"),
            col("__name__", "keyword"),
            col("instance", "keyword"),
            col("job", "keyword"),
            col("step", "long")
        );

        List<List<Object>> rows = List.of(
            List.of(List.of(1.5, 2.0), "http_requests_total", "localhost:9090", "prometheus", List.of(1735689600000L, 1735689660000L)),
            List.of(List.of(3.0, 4.0), "http_requests_total", "localhost:9091", "prometheus", List.of(1735689600000L, 1735689660000L))
        );

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "matrix",
                QueryMode.RANGE
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessMatrix(path);

            assertThat(path.evaluate("data.result"), hasSize(2));
            assertThat(path.evaluate("data.result.0.metric.__name__"), equalTo("http_requests_total"));
            assertThat(path.evaluate("data.result.0.metric.instance"), equalTo("localhost:9090"));
            assertThat(path.evaluate("data.result.0.metric.job"), equalTo("prometheus"));
            assertThat(path.evaluate("data.result.0.values.0"), equalTo(List.of(1735689600.0, "1.5")));
            assertThat(path.evaluate("data.result.0.values.1"), equalTo(List.of(1735689660.0, "2.0")));
            assertThat(path.evaluate("data.result.1.metric.__name__"), equalTo("http_requests_total"));
            assertThat(path.evaluate("data.result.1.metric.instance"), equalTo("localhost:9091"));
            assertThat(path.evaluate("data.result.1.metric.job"), equalTo("prometheus"));
            assertThat(path.evaluate("data.result.1.values.0"), equalTo(List.of(1735689600.0, "3.0")));
            assertThat(path.evaluate("data.result.1.values.1"), equalTo(List.of(1735689660.0, "4.0")));
        }
    }

    // A null label value (e.g. a BY label null-filled because it was absent from a series) must be OMITTED from the
    // Prometheus `metric` object, not serialized as an empty string. PromQL distinguishes an absent label from a label
    // whose value is "". This mirrors the `_timeseries` JSON path, which only emits non-null entries.
    public void testConvertRangeQueryOmitsNullIndividualLabels() throws IOException {
        List<ColumnInfoImpl> columns = List.of(
            col("value", "double"),
            col("__name__", "keyword"),
            col("instance", "keyword"),
            col("job", "keyword"),
            col("step", "long")
        );

        // Second series has no `instance` label (null), simulating a null-filled missing BY label.
        List<List<Object>> rows = Arrays.asList(
            List.of(List.of(1.5, 2.0), "http_requests_total", "localhost:9090", "prometheus", List.of(1735689600000L, 1735689660000L)),
            Arrays.asList(List.of(3.0, 4.0), "http_requests_total", null, "prometheus", List.of(1735689600000L, 1735689660000L))
        );

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "matrix",
                QueryMode.RANGE
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessMatrix(path);

            assertThat(path.evaluate("data.result"), hasSize(2));
            // First series keeps its present labels.
            assertThat(path.evaluate("data.result.0.metric.instance"), equalTo("localhost:9090"));
            // Second series: `instance` is null -> the key must be absent, not "".
            assertThat(path.evaluate("data.result.1.metric.__name__"), equalTo("http_requests_total"));
            assertThat(path.evaluate("data.result.1.metric.job"), equalTo("prometheus"));
            assertThat(path.evaluate("data.result.1.metric.instance"), nullValue());
        }
    }

    public void testConvertRangeQueryWithTimeseriesColumn() throws IOException {
        // The PROMQL command returns a _timeseries column with JSON format {"labels":{...}}
        // The listener extracts the inner labels as bare metric keys (no "labels." prefix)
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("_timeseries", "keyword"), col("step", "long"));

        List<List<Object>> rows = List.of(
            List.of(
                List.of(1.5, 2.0),
                "{\"labels\":{\"__name__\":\"http_requests_total\",\"job\":\"prometheus\"}}",
                List.of(1735689600000L, 1735689660000L)
            )
        );

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "matrix",
                QueryMode.RANGE
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessMatrix(path);

            assertThat(path.evaluate("data.result"), hasSize(1));
            assertThat(path.evaluate("data.result.0.metric.__name__"), equalTo("http_requests_total"));
            assertThat(path.evaluate("data.result.0.metric.job"), equalTo("prometheus"));
            assertThat(path.evaluate("data.result.0.values.0"), equalTo(List.of(1735689600.0, "1.5")));
            assertThat(path.evaluate("data.result.0.values.1"), equalTo(List.of(1735689660.0, "2.0")));
        }
    }

    public void testConvertRangeQueryWithTimeseriesColumnAttributesNamespace() throws IOException {
        // _timeseries JSON with an "attributes" namespace containing a nested object.
        // All non-"labels" namespaces are flattened recursively with dot-separated paths, so
        // {"attributes":{"resource":{"service.name":"my-service"}}} -> metric key "attributes.resource.service.name".
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("_timeseries", "keyword"), col("step", "long"));

        List<List<Object>> rows = List.of(
            List.of(
                List.of(1.5, 2.0),
                "{\"attributes\":{\"resource\":{\"service.name\":\"my-service\"}}}",
                List.of(1735689600000L, 1735689660000L)
            )
        );

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "matrix",
                QueryMode.RANGE
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessMatrix(path);

            assertThat(path.evaluate("data.result"), hasSize(1));
            assertThat(path.evaluate("data.result.0.metric.attributes\\.resource\\.service\\.name"), equalTo("my-service"));
            assertThat(path.evaluate("data.result.0.values.0"), equalTo(List.of(1735689600.0, "1.5")));
            assertThat(path.evaluate("data.result.0.values.1"), equalTo(List.of(1735689660.0, "2.0")));
        }
    }

    public void testConvertRangeQueryWithTimeseriesColumnTopLevelScalar() throws IOException {
        // _timeseries JSON with a top-level scalar field (no namespace object).
        // {"host":"my-host"} → metric key "host".
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("_timeseries", "keyword"), col("step", "long"));

        List<List<Object>> rows = List.of(List.of(List.of(1.5, 2.0), "{\"host\":\"my-host\"}", List.of(1735689600000L, 1735689660000L)));

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "matrix",
                QueryMode.RANGE
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessMatrix(path);

            assertThat(path.evaluate("data.result"), hasSize(1));
            assertThat(path.evaluate("data.result.0.metric.host"), equalTo("my-host"));
            assertThat(path.evaluate("data.result.0.values.0"), equalTo(List.of(1735689600.0, "1.5")));
            assertThat(path.evaluate("data.result.0.values.1"), equalTo(List.of(1735689660.0, "2.0")));
        }
    }

    public void testConvertRangeQueryEmptyResult() throws IOException {
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("step", "long"));

        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                List.of(),
                columns,
                ZoneOffset.UTC,
                "matrix",
                QueryMode.RANGE
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessMatrix(path);
            assertThat(path.evaluate("data.result"), empty());
        }
    }

    public void testTimestampConversionRange() throws IOException {
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("step", "long"));
        List<List<Object>> rows = List.of(List.of(1.0, 1735689600000L));

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "matrix",
                QueryMode.RANGE
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            // 2025-01-01T00:00:00.000Z = 1735689600 epoch seconds
            assertThat(path.evaluate("data.result.0.values.0"), equalTo(List.of(1735689600.0, "1.0")));
        }
    }

    public void testConvertRangeFoldableScalarQuery() throws IOException {
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("step", "long"));
        List<List<Object>> rows = List.of(List.of(List.of(42.0, 42.0), List.of(1735689600000L, 1735689660000L)));

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "matrix",
                QueryMode.RANGE
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessMatrix(path);
            assertThat(path.evaluate("data.result"), hasSize(1));
            assertThat(path.evaluate("data.result.0.metric"), equalTo(Map.of()));
            assertThat(path.evaluate("data.result.0.values.0"), equalTo(List.of(1735689600.0, "42.0")));
            assertThat(path.evaluate("data.result.0.values.1"), equalTo(List.of(1735689660.0, "42.0")));
        }
    }

    // -------------------------------------------------------------------------
    // Instant query (vector) tests
    // -------------------------------------------------------------------------

    public void testConvertInstantQueryWithIndividualLabels() throws IOException {
        List<ColumnInfoImpl> columns = List.of(
            col("value", "double"),
            col("__name__", "keyword"),
            col("instance", "keyword"),
            col("job", "keyword"),
            col("step", "long")
        );

        // Two series, one data point each
        List<List<Object>> rows = List.of(
            List.of(1.5, "http_requests_total", "localhost:9090", "prometheus", 1735689600000L),
            List.of(3.0, "http_requests_total", "localhost:9091", "prometheus", 1735689600000L)
        );

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "vector",
                QueryMode.INSTANT
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessVector(path);

            assertThat(path.evaluate("data.result"), hasSize(2));
            assertThat(path.evaluate("data.result.0.metric.__name__"), equalTo("http_requests_total"));
            assertThat(path.evaluate("data.result.0.metric.instance"), equalTo("localhost:9090"));
            assertThat(path.evaluate("data.result.0.metric.job"), equalTo("prometheus"));
            assertThat(path.evaluate("data.result.0.value"), equalTo(List.of(1735689600.0, "1.5")));
            assertThat(path.evaluate("data.result.1.metric.__name__"), equalTo("http_requests_total"));
            assertThat(path.evaluate("data.result.1.metric.instance"), equalTo("localhost:9091"));
            assertThat(path.evaluate("data.result.1.metric.job"), equalTo("prometheus"));
            assertThat(path.evaluate("data.result.1.value"), equalTo(List.of(1735689600.0, "3.0")));
        }
    }

    public void testConvertInstantQueryWithTimeseriesColumn() throws IOException {
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("_timeseries", "keyword"), col("step", "long"));

        List<List<Object>> rows = List.of(List.of(2.5, "{\"labels\":{\"__name__\":\"up\",\"job\":\"prometheus\"}}", 1735689600000L));

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "vector",
                QueryMode.INSTANT
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessVector(path);

            assertThat(path.evaluate("data.result"), hasSize(1));
            assertThat(path.evaluate("data.result.0.metric.__name__"), equalTo("up"));
            assertThat(path.evaluate("data.result.0.metric.job"), equalTo("prometheus"));
            assertThat(path.evaluate("data.result.0.value"), equalTo(List.of(1735689600.0, "2.5")));
        }
    }

    /**
     * Instant query responses use the last sample in the collapsed series.
     */
    public void testInstantQueryCollapsedSeriesKeepsLastSample() throws IOException {
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("job", "keyword"), col("step", "long"));

        List<List<Object>> rows = List.of(
            List.of(List.of(1.0, 2.0, 3.0), "prometheus", List.of(1735689600000L, 1735689660000L, 1735689720000L))
        );

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "vector",
                QueryMode.INSTANT
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessVector(path);

            assertThat(path.evaluate("data.result"), hasSize(1));
            // Last sample: value=3.0, timestamp=1735689720s
            assertThat(path.evaluate("data.result.0.value"), equalTo(List.of(1735689720.0, "3.0")));
        }
    }

    public void testConvertInstantQueryEmptyResult() throws IOException {
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("step", "long"));

        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                List.of(),
                columns,
                ZoneOffset.UTC,
                "vector",
                QueryMode.INSTANT
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessVector(path);
            assertThat(path.evaluate("data.result"), empty());
        }
    }

    public void testTimestampConversionInstant() throws IOException {
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("step", "long"));
        List<List<Object>> rows = List.of(List.of(1.0, 1735689600000L));

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "vector",
                QueryMode.INSTANT
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            // 2025-01-01T00:00:00.000Z = 1735689600 epoch seconds
            assertThat(path.evaluate("data.result.0.value"), equalTo(List.of(1735689600.0, "1.0")));
        }
    }

    public void testConvertInstantScalarQuery() throws IOException {
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("step", "long"));
        List<List<Object>> rows = List.of(List.of(3.0, 1735689600000L));

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "scalar",
                QueryMode.INSTANT
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertThat(path.evaluate("status"), equalTo("success"));
            assertThat(path.evaluate("data.resultType"), equalTo("scalar"));
            assertThat(path.evaluate("data.result"), equalTo(List.of(1735689600.0, "3.0")));
        }
    }

    public void testConvertInstantScalarQueryKeepsLastCollapsedSample() throws IOException {
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("step", "long"));
        List<List<Object>> rows = List.of(List.of(List.of(1.0, 2.0, 3.0), List.of(1735689600000L, 1735689660000L, 1735689720000L)));

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "scalar",
                QueryMode.INSTANT
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertThat(path.evaluate("data.resultType"), equalTo("scalar"));
            assertThat(path.evaluate("data.result"), equalTo(List.of(1735689720.0, "3.0")));
        }
    }

    // -------------------------------------------------------------------------
    // Limit tests
    // -------------------------------------------------------------------------

    public void testLimitTruncatesSeriesInRangeQuery() throws IOException {
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("job", "keyword"), col("step", "long"));

        // Three series — limit=2 should keep only the first two and add a warning
        List<List<Object>> rows = List.of(
            List.of(1.0, "a", 1735689600000L),
            List.of(2.0, "b", 1735689600000L),
            List.of(3.0, "c", 1735689600000L)
        );

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "matrix",
                QueryMode.RANGE,
                2
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessMatrix(path);
            assertThat(path.evaluate("data.result"), hasSize(2));
            assertThat(path.evaluate("warnings.0"), equalTo("results truncated due to limit"));
        }
    }

    public void testLimitTruncatesSeriesInInstantQuery() throws IOException {
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("job", "keyword"), col("step", "long"));

        List<List<Object>> rows = List.of(
            List.of(1.0, "a", 1735689600000L),
            List.of(2.0, "b", 1735689600000L),
            List.of(3.0, "c", 1735689600000L)
        );

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "vector",
                QueryMode.INSTANT,
                2
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessVector(path);
            assertThat(path.evaluate("data.result"), hasSize(2));
            assertThat(path.evaluate("warnings.0"), equalTo("results truncated due to limit"));
        }
    }

    public void testLimitDoesNotWarnWhenExactlyAtLimit() throws IOException {
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("job", "keyword"), col("step", "long"));

        List<List<Object>> rows = List.of(List.of(1.0, "a", 1735689600000L), List.of(2.0, "b", 1735689600000L));

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "matrix",
                QueryMode.RANGE,
                2
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessMatrix(path);
            assertThat(path.evaluate("data.result"), hasSize(2));
            assertThat(path.evaluate("warnings"), equalTo(null));
        }
    }

    public void testMaxValueLimitMeansNoLimit() throws IOException {
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("job", "keyword"), col("step", "long"));

        List<List<Object>> rows = List.of(
            List.of(1.0, "a", 1735689600000L),
            List.of(2.0, "b", 1735689600000L),
            List.of(3.0, "c", 1735689600000L)
        );

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "matrix",
                QueryMode.RANGE,
                Integer.MAX_VALUE
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessMatrix(path);
            assertThat(path.evaluate("data.result"), hasSize(3));
            assertThat(path.evaluate("warnings"), equalTo(null));
        }
    }

    public void testLimitKeepsAllSamplesForRemainingSeriesAfterTruncation() throws IOException {
        // Limits apply to collapsed series, not to samples within a series.
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("job", "keyword"), col("step", "long"));

        List<List<Object>> rows = List.of(
            List.of(List.of(1.0, 3.0), "a", List.of(1735689600000L, 1735689660000L)),
            List.of(2.0, "b", 1735689600000L)
        );

        List<Page> pages = pagesOf(rows);
        try (
            XContentBuilder builder = PrometheusQueryResponseListener.convertToPrometheusJson(
                pages,
                columns,
                ZoneOffset.UTC,
                "matrix",
                QueryMode.RANGE,
                1
            )
        ) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessMatrix(path);
            assertThat(path.evaluate("data.result"), hasSize(1));
            // The one kept series must have both its samples (even though a truncated series appeared between them)
            assertThat(path.evaluate("data.result.0.values"), hasSize(2));
            assertThat(path.evaluate("warnings.0"), equalTo("results truncated due to limit"));
        }
    }

    // -------------------------------------------------------------------------
    // Shared helper tests
    // -------------------------------------------------------------------------

    public void testFormatSampleValueNaN() {
        assertThat(PrometheusQueryResponseListener.formatSampleValue(Double.NaN), equalTo("NaN"));
    }

    public void testFormatSampleValueInfinity() {
        assertThat(PrometheusQueryResponseListener.formatSampleValue(Double.POSITIVE_INFINITY), equalTo("+Inf"));
        assertThat(PrometheusQueryResponseListener.formatSampleValue(Double.NEGATIVE_INFINITY), equalTo("-Inf"));
    }

    public void testBuildErrorJson() throws IOException {
        try (XContentBuilder builder = PrometheusErrorResponse.build(RestStatus.BAD_REQUEST, "test error")) {
            ObjectPath path = toObjectPath(builder);
            assertThat(path.evaluate("status"), equalTo("error"));
            assertThat(path.evaluate("errorType"), equalTo("bad_data"));
            assertThat(path.evaluate("error"), equalTo("test error"));
        }
    }

    public void testBuildErrorJsonTimeout() throws IOException {
        try (XContentBuilder builder = PrometheusErrorResponse.build(RestStatus.SERVICE_UNAVAILABLE, "timeout")) {
            ObjectPath path = toObjectPath(builder);
            assertThat(path.evaluate("errorType"), equalTo("timeout"));
        }
    }

    public void testMissingValueColumnThrows() {
        List<ColumnInfoImpl> columns = List.of(col("step", "date"));
        expectThrows(
            IllegalStateException.class,
            () -> PrometheusQueryResponseListener.convertToPrometheusJson(List.of(), columns, ZoneOffset.UTC, "matrix", QueryMode.RANGE)
        );
    }

    public void testMissingStepColumnThrows() {
        // Only value column — step is missing (would need to be last)
        List<ColumnInfoImpl> columns = List.of(col("value", "double"));
        expectThrows(
            IllegalStateException.class,
            () -> PrometheusQueryResponseListener.convertToPrometheusJson(List.of(), columns, ZoneOffset.UTC, "matrix", QueryMode.RANGE)
        );
    }

    public void testWrongLastColumnNameThrows() {
        // Last column is not named "step"
        List<ColumnInfoImpl> columns = List.of(col("value", "double"), col("timestamp", "long"));
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> PrometheusQueryResponseListener.convertToPrometheusJson(List.of(), columns, ZoneOffset.UTC, "matrix", QueryMode.RANGE)
        );
        assertThat(e.getMessage(), containsString("missing required 'step' column at last index"));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ColumnInfoImpl col(String name, String type) {
        return new ColumnInfoImpl(name, type, null);
    }

    /** Builds a single-page result from row-major test data, exercising the real Block-backed code path. */
    private static List<Page> pagesOf(List<List<Object>> rows) {
        if (rows.isEmpty()) {
            return List.of();
        }
        return List.of(new Page(BlockUtils.fromList(TestBlockFactory.getNonBreakingInstance(), rows)));
    }

    private static void assertSuccessMatrix(ObjectPath path) throws IOException {
        assertThat(path.evaluate("status"), equalTo("success"));
        assertThat(path.evaluate("data.resultType"), equalTo("matrix"));
    }

    private static void assertSuccessVector(ObjectPath path) throws IOException {
        assertThat(path.evaluate("status"), equalTo("success"));
        assertThat(path.evaluate("data.resultType"), equalTo("vector"));
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
