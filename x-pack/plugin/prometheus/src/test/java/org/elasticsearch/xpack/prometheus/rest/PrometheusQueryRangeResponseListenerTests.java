/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PrometheusQueryRangeResponseListenerTests extends ESTestCase {

    // Column names are bare label names (e.g. "job", "instance") — no "labels." prefix.
    // The Prometheus data stream maps labels with type: passthrough, so PROMQL resolves
    // labels.job → column "job", labels.instance → column "instance", etc.
    public void testConvertRangeQueryWithIndividualLabels() throws IOException {
        List<TestColumnInfo> columns = List.of(
            new TestColumnInfo("value", "double"),
            new TestColumnInfo("__name__", "keyword"),
            new TestColumnInfo("instance", "keyword"),
            new TestColumnInfo("job", "keyword"),
            new TestColumnInfo("step", "long")
        );

        List<List<Object>> rows = List.of(
            List.of(1.5, "http_requests_total", "localhost:9090", "prometheus", 1735689600000L),
            List.of(2.0, "http_requests_total", "localhost:9090", "prometheus", 1735689660000L),
            List.of(3.0, "http_requests_total", "localhost:9091", "prometheus", 1735689600000L),
            List.of(4.0, "http_requests_total", "localhost:9091", "prometheus", 1735689660000L)
        );

        EsqlResponse response = new TestEsqlResponse(columns, rows);
        try (XContentBuilder builder = PrometheusQueryRangeResponseListener.convertToPrometheusJson(response)) {
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

    public void testConvertRangeQueryWithTimeseriesColumn() throws IOException {
        // The PROMQL command returns a _timeseries column with JSON format {"labels":{...}}
        // The listener extracts the inner labels as bare metric keys (no "labels." prefix)
        List<TestColumnInfo> columns = List.of(
            new TestColumnInfo("value", "double"),
            new TestColumnInfo("_timeseries", "keyword"),
            new TestColumnInfo("step", "long")
        );

        List<List<Object>> rows = List.of(
            List.of(1.5, "{\"labels\":{\"__name__\":\"http_requests_total\",\"job\":\"prometheus\"}}", 1735689600000L),
            List.of(2.0, "{\"labels\":{\"__name__\":\"http_requests_total\",\"job\":\"prometheus\"}}", 1735689660000L)
        );

        EsqlResponse response = new TestEsqlResponse(columns, rows);
        try (XContentBuilder builder = PrometheusQueryRangeResponseListener.convertToPrometheusJson(response)) {
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
        List<TestColumnInfo> columns = List.of(
            new TestColumnInfo("value", "double"),
            new TestColumnInfo("_timeseries", "keyword"),
            new TestColumnInfo("step", "long")
        );

        List<List<Object>> rows = List.of(
            List.of(1.5, "{\"attributes\":{\"resource\":{\"service.name\":\"my-service\"}}}", 1735689600000L),
            List.of(2.0, "{\"attributes\":{\"resource\":{\"service.name\":\"my-service\"}}}", 1735689660000L)
        );

        EsqlResponse response = new TestEsqlResponse(columns, rows);
        try (XContentBuilder builder = PrometheusQueryRangeResponseListener.convertToPrometheusJson(response)) {
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
        List<TestColumnInfo> columns = List.of(
            new TestColumnInfo("value", "double"),
            new TestColumnInfo("_timeseries", "keyword"),
            new TestColumnInfo("step", "long")
        );

        List<List<Object>> rows = List.of(
            List.of(1.5, "{\"host\":\"my-host\"}", 1735689600000L),
            List.of(2.0, "{\"host\":\"my-host\"}", 1735689660000L)
        );

        EsqlResponse response = new TestEsqlResponse(columns, rows);
        try (XContentBuilder builder = PrometheusQueryRangeResponseListener.convertToPrometheusJson(response)) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessMatrix(path);

            assertThat(path.evaluate("data.result"), hasSize(1));
            assertThat(path.evaluate("data.result.0.metric.host"), equalTo("my-host"));
            assertThat(path.evaluate("data.result.0.values.0"), equalTo(List.of(1735689600.0, "1.5")));
            assertThat(path.evaluate("data.result.0.values.1"), equalTo(List.of(1735689660.0, "2.0")));
        }
    }

    public void testConvertEmptyResult() throws IOException {
        List<TestColumnInfo> columns = List.of(new TestColumnInfo("value", "double"), new TestColumnInfo("step", "long"));

        EsqlResponse response = new TestEsqlResponse(columns, List.of());
        try (XContentBuilder builder = PrometheusQueryRangeResponseListener.convertToPrometheusJson(response)) {
            ObjectPath path = toObjectPath(builder);
            assertSuccessMatrix(path);

            assertThat(path.evaluate("data.result"), empty());
        }
    }

    public void testTimestampConversion() throws IOException {
        List<TestColumnInfo> columns = List.of(new TestColumnInfo("value", "double"), new TestColumnInfo("step", "long"));
        List<List<Object>> rows = List.of(List.of(1.0, 1735689600000L));

        EsqlResponse response = new TestEsqlResponse(columns, rows);
        try (XContentBuilder builder = PrometheusQueryRangeResponseListener.convertToPrometheusJson(response)) {
            ObjectPath path = toObjectPath(builder);
            // 2025-01-01T00:00:00.000Z = 1735689600 epoch seconds
            assertThat(path.evaluate("data.result.0.values.0"), equalTo(List.of(1735689600.0, "1.0")));
        }
    }

    public void testFormatSampleValueNaN() {
        assertThat(PrometheusQueryRangeResponseListener.formatSampleValue(Double.NaN), equalTo("NaN"));
    }

    public void testFormatSampleValueInfinity() {
        assertThat(PrometheusQueryRangeResponseListener.formatSampleValue(Double.POSITIVE_INFINITY), equalTo("+Inf"));
        assertThat(PrometheusQueryRangeResponseListener.formatSampleValue(Double.NEGATIVE_INFINITY), equalTo("-Inf"));
    }

    public void testFormatSampleValueNull() {
        assertThat(PrometheusQueryRangeResponseListener.formatSampleValue(null), equalTo("NaN"));
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
        List<TestColumnInfo> columns = List.of(new TestColumnInfo("step", "date"));
        EsqlResponse response = new TestEsqlResponse(columns, List.of());
        expectThrows(IllegalStateException.class, () -> PrometheusQueryRangeResponseListener.convertToPrometheusJson(response));
    }

    public void testMissingStepColumnThrows() {
        // Only value column — step is missing (would need to be last)
        List<TestColumnInfo> columns = List.of(new TestColumnInfo("value", "double"));
        EsqlResponse response = new TestEsqlResponse(columns, List.of());
        expectThrows(IllegalStateException.class, () -> PrometheusQueryRangeResponseListener.convertToPrometheusJson(response));
    }

    public void testWrongLastColumnNameThrows() {
        // Last column is not named "step"
        List<TestColumnInfo> columns = List.of(new TestColumnInfo("value", "double"), new TestColumnInfo("timestamp", "long"));
        EsqlResponse response = new TestEsqlResponse(columns, List.of());
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> PrometheusQueryRangeResponseListener.convertToPrometheusJson(response)
        );
        assertThat(e.getMessage(), containsString("missing required 'step' column at last index"));
    }

    private static void assertSuccessMatrix(ObjectPath path) throws IOException {
        assertThat(path.evaluate("status"), equalTo("success"));
        assertThat(path.evaluate("data.resultType"), equalTo("matrix"));
    }

    private static ObjectPath toObjectPath(XContentBuilder builder) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(builder).streamInput())
        ) {
            return new ObjectPath(parser.map());
        }
    }

    record TestColumnInfo(String name, String outputType) implements ColumnInfo {
        @Override
        public org.elasticsearch.xcontent.XContentBuilder toXContent(
            org.elasticsearch.xcontent.XContentBuilder builder,
            org.elasticsearch.xcontent.ToXContent.Params params
        ) {
            return builder;
        }

        @Override
        public void writeTo(org.elasticsearch.common.io.stream.StreamOutput out) {}
    }

    static class TestEsqlResponse implements EsqlResponse {
        private final List<? extends ColumnInfo> columns;
        private final List<List<Object>> rows;

        TestEsqlResponse(List<? extends ColumnInfo> columns, List<List<Object>> rows) {
            this.columns = columns;
            this.rows = rows;
        }

        @Override
        public List<? extends ColumnInfo> columns() {
            return columns;
        }

        @Override
        public Iterable<Iterable<Object>> rows() {
            List<Iterable<Object>> result = new ArrayList<>();
            for (List<Object> row : rows) {
                result.add(row);
            }
            return result;
        }

        @Override
        public Iterable<Object> column(int columnIndex) {
            List<Object> col = new ArrayList<>();
            for (List<Object> row : rows) {
                col.add(row.get(columnIndex));
            }
            return col;
        }

        @Override
        public void close() {}
    }
}
