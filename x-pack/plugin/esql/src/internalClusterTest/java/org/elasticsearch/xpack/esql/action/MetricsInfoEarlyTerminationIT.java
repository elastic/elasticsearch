/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.operator.MetricsInfoOperator;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;

/** Verifies that METRICS_INFO can stop source reads without losing metadata or bypassing filters. */
public class MetricsInfoEarlyTerminationIT extends AbstractEsqlIntegTestCase {
    private static final long TIMESTAMP = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");

    /** Sorting and filtering the catalogue must retain complete metadata while source reads stop early. */
    public void testStopsReadingAfterAllMetricMetadataIsObserved() {
        int numSeries = 4096;
        for (String index : List.of("metrics-a", "metrics-b")) {
            createMetricsIndex(index);
            var bulk = client().prepareBulk();
            for (int host = 0; host < numSeries; host++) {
                bulk.add(
                    client().prepareIndex(index).setSource("@timestamp", TIMESTAMP, "host", index + "-h" + host, "cpu", 0.5, "disk", 10)
                );
            }
            var result = bulk.get();
            assertFalse(result.buildFailureMessage(), result.hasFailures());
        }
        client().admin().indices().prepareRefresh("metrics-a", "metrics-b").get();

        for (String suffix : List.of(" | SORT metric_name", " | WHERE metric_type == \"counter\" | SORT metric_name")) {
            try (var response = run(profiledRequest("TS metrics-* | METRICS_INFO" + suffix))) {
                List<List<Object>> rows = EsqlTestUtils.getValuesList(response);
                assertThat(rows, hasSize(suffix.contains("WHERE") ? 1 : 2));
                assertThat(
                    new HashSet<>(rows.stream().map(row -> row.get(0)).toList()),
                    equalTo(suffix.contains("WHERE") ? Set.of("disk") : Set.of("cpu", "disk"))
                );
                for (List<Object> row : rows) {
                    boolean cpu = row.get(0).equals("cpu");
                    assertThat(values(row.get(1)), equalTo(Set.of("metrics-a", "metrics-b")));
                    assertThat(values(row.get(2)), equalTo(Set.of(cpu ? "percent" : "bytes")));
                    assertThat(values(row.get(3)), equalTo(Set.of(cpu ? "gauge" : "counter")));
                    assertThat(values(row.get(4)), equalTo(Set.of(cpu ? "double" : "long")));
                    assertThat(values(row.get(5)), equalTo(Set.of("host")));
                }
                long sourceRows = sourceRows(response);
                assertThat(sourceRows, greaterThan(0L));
                assertThat(sourceRows, lessThan(2L * numSeries));
            }
        }
    }

    /** Mapped metrics that have no matching documents must never be added to the result. */
    public void testFilterDoesNotInventMappedMetrics() {
        createMetricsIndex("metrics-filtered");
        var bulk = client().prepareBulk();
        bulk.add(client().prepareIndex("metrics-filtered").setSource("@timestamp", TIMESTAMP, "host", "keep", "cpu", 0.5));
        bulk.add(client().prepareIndex("metrics-filtered").setSource("@timestamp", TIMESTAMP, "host", "drop", "disk", 10));
        var result = bulk.get();
        assertFalse(result.buildFailureMessage(), result.hasFailures());
        client().admin().indices().prepareRefresh("metrics-filtered").get();

        try (var response = run(profiledRequest("TS metrics-filtered | WHERE host == \"keep\" | METRICS_INFO"))) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo("cpu"));
            assertThat(values(rows.get(0).get(2)), equalTo(Set.of("percent")));
            assertThat(values(rows.get(0).get(5)), equalTo(Set.of("host")));
        }
    }

    /** Flattened dimensions may expose additional leaf names, so their metadata requires the full scan. */
    public void testFlattenedDimensionsKeepReadingAllSeries() {
        client().admin()
            .indices()
            .prepareCreate("metrics-flattened")
            .setSettings(
                Settings.builder()
                    .put("index.mode", "time_series")
                    .putList("index.routing_path", "labels.host")
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            )
            .setMapping("""
                {"properties": {
                  "@timestamp": {"type": "date"},
                  "labels": {"type": "flattened", "time_series_dimensions": ["host"]},
                  "cpu": {"type": "double", "time_series_metric": "gauge"}
                }}
                """)
            .get();
        int numSeries = 64;
        var bulk = client().prepareBulk();
        for (int host = 0; host < numSeries; host++) {
            Map<String, String> labels = host == numSeries - 1
                ? Map.of("host", "h" + host, "extra", "present")
                : Map.of("host", "h" + host);
            bulk.add(client().prepareIndex("metrics-flattened").setSource("@timestamp", TIMESTAMP, "labels", labels, "cpu", 0.5));
        }
        var result = bulk.get();
        assertFalse(result.buildFailureMessage(), result.hasFailures());
        client().admin().indices().prepareRefresh("metrics-flattened").get();

        try (var response = run(profiledRequest("TS metrics-flattened | METRICS_INFO"))) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(values(rows.get(0).get(5)), equalTo(Set.of("labels.host", "labels.extra")));
            assertThat(sourceRows(response), equalTo((long) numSeries));
        }
    }

    /** The first query must stop on a dense mapping without relying on a previously cached catalogue. */
    public void testColdDenseMappingStopsBeforeAllDocuments() {
        int metricCount = 128;
        int numDocuments = 1024;
        Map<String, Object> metrics = new HashMap<>();
        Set<String> expectedMetricNames = new HashSet<>();
        for (int metric = 0; metric < metricCount; metric++) {
            String name = "metric_" + metric;
            metrics.put(name, Map.of("type", "double", "time_series_metric", "gauge", "meta", Map.of("unit", "count")));
            expectedMetricNames.add(name);
        }
        createMetricsIndex("metrics-dense", metrics);
        var bulk = client().prepareBulk();
        for (int host = 0; host < numDocuments; host++) {
            Map<String, Object> document = new HashMap<>();
            document.put("@timestamp", TIMESTAMP);
            document.put("host", "h" + host);
            for (String metric : expectedMetricNames) {
                document.put(metric, 1.0);
            }
            bulk.add(client().prepareIndex("metrics-dense").setSource(document));
        }
        var result = bulk.get();
        assertFalse(result.buildFailureMessage(), result.hasFailures());
        client().admin().indices().prepareRefresh("metrics-dense").get();

        try (var response = run(profiledRequest("TS metrics-dense | METRICS_INFO | SORT metric_name"))) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(response);
            assertThat(rows, hasSize(metricCount));
            assertThat(new HashSet<>(rows.stream().map(row -> row.get(0)).toList()), equalTo(expectedMetricNames));
            for (List<Object> row : rows) {
                assertThat(values(row.get(1)), equalTo(Set.of("metrics-dense")));
                assertThat(values(row.get(2)), equalTo(Set.of("count")));
                assertThat(values(row.get(3)), equalTo(Set.of("gauge")));
                assertThat(values(row.get(4)), equalTo(Set.of("double")));
                assertThat(values(row.get(5)), equalTo(Set.of("host")));
            }
            reportWorkCounts("dense mapping", numDocuments, response);
            assertThat(sourceRows(response), greaterThan(0L));
            assertThat(sourceRows(response), lessThan((long) numDocuments));
            assertThat(initialRows(response), greaterThan(0L));
            assertThat(initialRows(response), lessThan((long) numDocuments));
        }
    }

    /** Absent fields in a large mapping must prevent completion without inventing metadata. */
    public void testSparseLargeMappingScansWithoutInventingMetrics() {
        int numDocuments = 1024;
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("cpu", Map.of("type", "double", "time_series_metric", "gauge", "meta", Map.of("unit", "percent")));
        metrics.put("disk", Map.of("type", "long", "time_series_metric", "counter", "meta", Map.of("unit", "bytes")));
        for (int metric = 0; metric < 512; metric++) {
            metrics.put("unused_" + metric, Map.of("type", "double", "time_series_metric", "gauge"));
        }
        createMetricsIndex("metrics-sparse", metrics);
        var bulk = client().prepareBulk();
        for (int host = 0; host < numDocuments; host++) {
            String metric = host == numDocuments - 1 ? "disk" : "cpu";
            bulk.add(client().prepareIndex("metrics-sparse").setSource("@timestamp", TIMESTAMP, "host", "h" + host, metric, 1));
        }
        var result = bulk.get();
        assertFalse(result.buildFailureMessage(), result.hasFailures());
        client().admin().indices().prepareRefresh("metrics-sparse").get();

        try (var response = run(profiledRequest("TS metrics-sparse | METRICS_INFO | SORT metric_name"))) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo("cpu"));
            assertThat(values(rows.get(0).get(2)), equalTo(Set.of("percent")));
            assertThat(rows.get(1).get(0), equalTo("disk"));
            assertThat(values(rows.get(1).get(2)), equalTo(Set.of("bytes")));
            reportWorkCounts("stale mapping", numDocuments, response);
            assertThat(sourceRows(response), equalTo((long) numDocuments));
            assertThat(initialRows(response), equalTo((long) numDocuments));
        }
    }

    /** A series may contain different metric fields at different timestamps. */
    public void testSparseMetricSetWithinSeriesIsComplete() {
        createMetricsIndex("metrics-changing-series");
        var bulk = client().prepareBulk();
        bulk.add(client().prepareIndex("metrics-changing-series").setSource("@timestamp", TIMESTAMP, "host", "same", "cpu", 0.5));
        bulk.add(client().prepareIndex("metrics-changing-series").setSource("@timestamp", TIMESTAMP + 1000, "host", "same", "disk", 10));
        var result = bulk.get();
        assertFalse(result.buildFailureMessage(), result.hasFailures());
        client().admin().indices().prepareRefresh("metrics-changing-series").get();

        try (var response = run(profiledRequest("TS metrics-changing-series | METRICS_INFO | SORT metric_name"))) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo("cpu"));
            assertThat(rows.get(1).get(0), equalTo("disk"));
            for (List<Object> row : rows) {
                assertThat(values(row.get(5)), equalTo(Set.of("host")));
            }
        }
        try (
            var response = run(
                profiledRequest(
                    "TS metrics-changing-series | WHERE @timestamp < \"2024-04-15T00:00:00.500Z\" | METRICS_INFO | SORT metric_name"
                )
            )
        ) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo("cpu"));
        }
    }

    /** An unobserved mapped metric requires checking every document even when their TSIDs are identical. */
    public void testRepeatedSeriesFallbackProcessesEveryDocument() {
        createMetricsIndex("metrics-repeated");
        int numDocuments = 512;
        var bulk = client().prepareBulk();
        for (int document = 0; document < numDocuments; document++) {
            bulk.add(
                client().prepareIndex("metrics-repeated").setSource("@timestamp", TIMESTAMP + document * 1000L, "host", "same", "cpu", 0.5)
            );
        }
        var result = bulk.get();
        assertFalse(result.buildFailureMessage(), result.hasFailures());
        client().admin().indices().prepareRefresh("metrics-repeated").get();

        try (var response = run(profiledRequest("TS metrics-repeated | METRICS_INFO | SORT metric_name"))) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo("cpu"));
            reportWorkCounts("repeated series fallback", numDocuments, response);
            assertThat(sourceRows(response), equalTo((long) numDocuments));
            assertThat(initialRows(response), equalTo((long) numDocuments));
        }
    }

    private void reportWorkCounts(String scenario, int documents, EsqlQueryResponse response) {
        logger.info(
            "METRICS_INFO work counts [{}]: matching documents [{}], source rows [{}], metadata rows [{}]",
            scenario,
            documents,
            sourceRows(response),
            initialRows(response)
        );
    }

    private void createMetricsIndex(String index, Map<String, Object> metrics) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("@timestamp", Map.of("type", "date"));
        properties.put("host", Map.of("type", "keyword", "time_series_dimension", true));
        properties.putAll(metrics);
        client().admin()
            .indices()
            .prepareCreate(index)
            .setSettings(
                Settings.builder()
                    .put("index.mode", "time_series")
                    .putList("index.routing_path", "host")
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            )
            .setMapping(Map.of("properties", properties))
            .get();
    }

    private void createMetricsIndex(String index) {
        client().admin()
            .indices()
            .prepareCreate(index)
            .setSettings(
                Settings.builder()
                    .put("index.mode", "time_series")
                    .putList("index.routing_path", "host")
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            )
            .setMapping("""
                {"properties": {
                  "@timestamp": {"type": "date"},
                  "host": {"type": "keyword", "time_series_dimension": true},
                  "cpu": {"type": "double", "time_series_metric": "gauge", "meta": {"unit": "percent"}},
                  "disk": {"type": "long", "time_series_metric": "counter", "meta": {"unit": "bytes"}}
                }}
                """)
            .get();
    }

    private static EsqlQueryRequest profiledRequest(String query) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.profile(true);
        // Small pages make the work-count assertions independent of the source's default page size.
        request.pragmas(new QueryPragmas(Settings.builder().put("page_size", 16).put("task_concurrency", 2).build()));
        request.acceptedPragmaRisks(true);
        return request;
    }

    private static long initialRows(EsqlQueryResponse response) {
        return response.profile()
            .drivers()
            .stream()
            .flatMap(driver -> driver.operators().stream())
            .filter(
                operator -> operator.status() instanceof MetricsInfoOperator.Status status
                    && status.mode() == MetricsInfoOperator.Mode.INITIAL
            )
            .mapToLong(operator -> ((MetricsInfoOperator.Status) operator.status()).rowsReceived())
            .sum();
    }

    private static long sourceRows(EsqlQueryResponse response) {
        return response.profile()
            .drivers()
            .stream()
            .flatMap(driver -> driver.operators().stream())
            .filter(operator -> operator.status() instanceof LuceneSourceOperator.Status)
            .mapToLong(operator -> ((LuceneSourceOperator.Status) operator.status()).rowsEmitted())
            .sum();
    }

    private static Set<?> values(Object value) {
        return value instanceof List<?> list ? new HashSet<>(list) : Set.of(value);
    }
}
