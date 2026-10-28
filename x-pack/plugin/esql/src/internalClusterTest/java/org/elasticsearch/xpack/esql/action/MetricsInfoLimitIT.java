/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.MetricsInfoOperator;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/** Verifies LIMIT-driven sampling without assuming a complete metric catalogue. */
public class MetricsInfoLimitIT extends AbstractEsqlIntegTestCase {
    /** The source must stop early when a small limit bounds the number of sampled series. */
    public void testLimitBoundsSampledSeries() {
        int numSeries = 4096;
        createMetricsIndex(numSeries);
        int minRows = between(1, 5);
        int multiplier = between(1, 3);
        int limit = between(1, 2);
        int sampleRows = Math.max(minRows, limit * multiplier);
        updateClusterSettings(
            Settings.builder()
                .put(PlannerSettings.METRICS_INFO_SAMPLE_MIN_ROWS.getKey(), minRows)
                .put(PlannerSettings.METRICS_INFO_SAMPLE_LIMIT_MULTIPLIER.getKey(), multiplier)
        );
        try {
            try (var response = run(profiledRequest("TS metrics-sample | METRICS_INFO | LIMIT " + limit))) {
                assertThat(EsqlTestUtils.getValuesList(response), hasSize(limit));
                int initialOperators = 0;
                for (DriverProfile driver : response.profile().drivers()) {
                    for (OperatorStatus operator : driver.operators()) {
                        if (operator.status() instanceof MetricsInfoOperator.Status status) {
                            if (status.mode() == MetricsInfoOperator.Mode.INITIAL) {
                                initialOperators++;
                                assertThat(status.rowsReceived(), lessThanOrEqualTo((long) sampleRows));
                            } else {
                                assertThat(status.rowsEmitted(), lessThanOrEqualTo((long) limit));
                            }
                        }
                    }
                }
                assertThat(initialOperators, greaterThan(0));
                long sourceRows = sourceRows(response);
                assertThat(sourceRows, greaterThan(0L));
                assertThat(sourceRows, lessThan((long) numSeries));
            }
        } finally {
            updateClusterSettings(
                Settings.builder()
                    .putNull(PlannerSettings.METRICS_INFO_SAMPLE_MIN_ROWS.getKey())
                    .putNull(PlannerSettings.METRICS_INFO_SAMPLE_LIMIT_MULTIPLIER.getKey())
            );
        }
    }

    /** Filtering or sorting metadata above METRICS_INFO must block the sampling hint. */
    public void testFilterAndSortKeepFullScan() {
        int numSeries = 128;
        createMetricsIndex(numSeries);
        updateClusterSettings(
            Settings.builder()
                .put(PlannerSettings.METRICS_INFO_SAMPLE_MIN_ROWS.getKey(), 1)
                .put(PlannerSettings.METRICS_INFO_SAMPLE_LIMIT_MULTIPLIER.getKey(), 1)
        );
        try {
            try (
                var response = run(
                    profiledRequest("TS metrics-sample | METRICS_INFO | WHERE metric_type == \"counter\" | SORT metric_name | LIMIT 1")
                )
            ) {
                assertThat(EsqlTestUtils.getValuesList(response), hasSize(1));
                assertThat(EsqlTestUtils.getValuesList(response).get(0).get(0), equalTo("disk"));
                long initialRows = response.profile()
                    .drivers()
                    .stream()
                    .flatMap(driver -> driver.operators().stream())
                    .filter(
                        operator -> operator.status() instanceof MetricsInfoOperator.Status status
                            && status.mode() == MetricsInfoOperator.Mode.INITIAL
                    )
                    .mapToLong(operator -> ((MetricsInfoOperator.Status) operator.status()).rowsReceived())
                    .sum();
                assertThat(initialRows, equalTo((long) numSeries));
                assertThat(sourceRows(response), equalTo((long) numSeries));
            }
        } finally {
            updateClusterSettings(
                Settings.builder()
                    .putNull(PlannerSettings.METRICS_INFO_SAMPLE_MIN_ROWS.getKey())
                    .putNull(PlannerSettings.METRICS_INFO_SAMPLE_LIMIT_MULTIPLIER.getKey())
            );
        }
    }

    private void createMetricsIndex(int numSeries) {
        client().admin()
            .indices()
            .prepareCreate("metrics-sample")
            .setSettings(
                Settings.builder()
                    .put("index.mode", "time_series")
                    .putList("index.routing_path", "host")
                    .put("index.number_of_shards", between(2, 5))
                    .put("index.number_of_replicas", 0)
            )
            .setMapping("""
                {"properties": {
                  "@timestamp": {"type": "date"},
                  "host": {"type": "keyword", "time_series_dimension": true},
                  "cpu": {"type": "double", "time_series_metric": "gauge"},
                  "disk": {"type": "long", "time_series_metric": "counter"},
                  "memory": {"type": "long", "time_series_metric": "gauge"}
                }}
                """)
            .get();
        long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        var bulk = client().prepareBulk();
        for (int host = 0; host < numSeries; host++) {
            bulk.add(
                client().prepareIndex("metrics-sample")
                    .setSource("@timestamp", timestamp, "host", "h" + host, "cpu", 0.5, "disk", 10, "memory", 100)
            );
        }
        var result = bulk.get();
        assertFalse(result.buildFailureMessage(), result.hasFailures());
        client().admin().indices().prepareRefresh("metrics-sample").get();
    }

    private static EsqlQueryRequest profiledRequest(String query) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.profile(true);
        request.pragmas(new QueryPragmas(Settings.builder().put("page_size", 16).put("task_concurrency", 2).build()));
        request.acceptedPragmaRisks(true);
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
}
