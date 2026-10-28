/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

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
        if (canUseQueryPragmas()) {
            request.pragmas(new QueryPragmas(Settings.builder().put("page_size", 16).put("task_concurrency", 2).build()));
            request.acceptedPragmaRisks(true);
        }
        return request;
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
