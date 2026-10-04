/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Where PromQL unsets labels from a series' {@code _timeseries}, on a cluster of several data nodes and shards: once per
 * series on the data node, after the series' dimensions are read once, and above the data nodes only when the
 * {@code _timeseries} was already regrouped. Every result also equals the same aggregation grouped {@code by} the remaining
 * labels, a translation that never touches {@code _timeseries}.
 */
public class PromqlTimeSeriesUnsetIT extends AbstractEsqlIntegTestCase {

    private static final String RANGE = "start=\"2024-04-15T00:00:00Z\" end=\"2024-04-15T01:00:00Z\" step=10m";
    private static final int STEPS = 7;
    private static final List<String> LE = List.of("0.1", "0.5", "1", "+Inf");

    private int metricSeries;
    private int metricDocs;

    @Before
    public void setUpIndices() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        long start = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        // the same series in two backing indices, one per half hour, as a rollover leaves them
        createTimeSeriesIndex("promql-metrics-1", "2024-04-15T00:00:00Z", "2024-04-15T00:30:00Z", """
            {"properties":{"@timestamp":{"type":"date"},"cluster":{"type":"keyword","time_series_dimension":true},
            "region":{"type":"keyword","time_series_dimension":true},"pod":{"type":"keyword","time_series_dimension":true},
            "cpu":{"type":"double","time_series_metric":"gauge"}}}""", "cluster", "pod");
        createTimeSeriesIndex("promql-metrics-2", "2024-04-15T00:30:00Z", "2024-04-15T02:00:00Z", """
            {"properties":{"@timestamp":{"type":"date"},"cluster":{"type":"keyword","time_series_dimension":true},
            "region":{"type":"keyword","time_series_dimension":true},"pod":{"type":"keyword","time_series_dimension":true},
            "cpu":{"type":"double","time_series_metric":"gauge"}}}""", "cluster", "pod");
        BulkRequestBuilder metrics = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        metricSeries = 0;
        metricDocs = 0;
        for (String cluster : List.of("prod", "qa", "staging")) {
            String region = randomFrom("eu", "us");
            int pods = between(2, 4);
            for (int pod = 0; pod < pods; pod++) {
                metricSeries++;
                for (int minute = 0; minute < 60; minute++) {
                    metrics.add(
                        client().prepareIndex(minute < 30 ? "promql-metrics-1" : "promql-metrics-2")
                            .setCreate(true)
                            .setSource(
                                "@timestamp",
                                start + minute * 60_000L,
                                "cluster",
                                cluster,
                                "region",
                                region,
                                "pod",
                                "pod-" + pod,
                                "cpu",
                                randomIntBetween(0, 100)
                            )
                    );
                    metricDocs++;
                }
            }
        }
        assertFalse(metrics.get().hasFailures());

        createTimeSeriesIndex("promql-buckets", "2024-04-15T00:00:00Z", "2024-04-15T02:00:00Z", """
            {"properties":{"@timestamp":{"type":"date"},"job":{"type":"keyword","time_series_dimension":true},
            "instance":{"type":"keyword","time_series_dimension":true},"le":{"type":"keyword","time_series_dimension":true},
            "bucket":{"type":"double","time_series_metric":"counter"}}}""", "job", "instance", "le");
        BulkRequestBuilder buckets = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (String job : List.of("api", "worker")) {
            for (int instance = 0; instance < 3; instance++) {
                long[] counts = new long[LE.size()];
                for (int minute = 0; minute < 60; minute++) {
                    // each bucket grows at least as much as the one below it, so the buckets stay cumulative
                    long increment = 0;
                    for (int b = 0; b < LE.size(); b++) {
                        increment += randomIntBetween(0, 5);
                        counts[b] += increment;
                        buckets.add(
                            client().prepareIndex("promql-buckets")
                                .setCreate(true)
                                .setSource(
                                    "@timestamp",
                                    start + minute * 60_000L,
                                    "job",
                                    job,
                                    "instance",
                                    "i-" + instance,
                                    "le",
                                    LE.get(b),
                                    "bucket",
                                    counts[b]
                                )
                        );
                    }
                }
            }
        }
        assertFalse(buckets.get().hasFailures());
    }

    private void createTimeSeriesIndex(String index, String startTime, String endTime, String mapping, String... routingPath) {
        Settings settings = Settings.builder()
            .put("mode", "time_series")
            .putList("routing_path", routingPath)
            .put("time_series.start_time", startTime)
            .put("time_series.end_time", endTime)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(2, 4))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        client().admin().indices().prepareCreate(index).setSettings(settings).setMapping(mapping).get();
        ensureGreen(index);
    }

    /**
     * {@code without} over a raw selector: each data node unsets {@code pod} once per series, after the aggregate read the
     * series' dimensions, and never reads {@code _timeseries} per document; the coordinator unsets nothing.
     */
    public void testUnsetsOncePerSeriesOnTheDataNode() {
        try (EsqlQueryResponse without = run(request("promql-metrics-*", "sum without (pod) (cpu)"))) {
            Map<String, Double> expected = byLabels(
                run(request("promql-metrics-*", "sum by (cluster, region) (cpu)")),
                "cluster",
                "region"
            );
            assertThat(byTimeSeries(without, "cluster", "region"), matchesValues(expected));

            List<DriverProfile> data = drivers(without, "data");
            long unsetRows = 0;
            for (DriverProfile driver : data) {
                int aggregation = indexOf(driver, "TimeSeriesAggregationOperator");
                int unset = indexOf(driver, "TimeSeriesUnsetEvaluator");
                if (unset < 0) {
                    continue;
                }
                assertThat(driver.operators().get(unset).operator(), containsString("dimensions=[pod]"));
                assertThat("the unset runs after the per-series aggregate", unset, greaterThan(aggregation));
                for (int i = 0; i < aggregation; i++) {
                    assertNoTimeSeriesRead(driver.operators().get(i));
                }
                unsetRows += ((AbstractPageMappingOperator.Status) driver.operators().get(unset).status()).rowsReceived();
            }
            assertThat("some data node unsets pod", unsetRows, greaterThan(0L));
            // once per series and step on each shard holding it: never once per document
            assertThat(unsetRows, lessThanOrEqualTo(2L * metricSeries * STEPS));
            assertThat(unsetRows, lessThanOrEqualTo((long) metricDocs / 2));
            for (DriverProfile driver : drivers(without, "final")) {
                assertThat("the coordinator unsets nothing", indexOf(driver, "TimeSeriesUnsetEvaluator"), equalTo(-1));
            }
        }
    }

    /**
     * A classic histogram unsets {@code le} on the data nodes, once per series; a {@code without} over it unsets
     * {@code instance} from the {@code _timeseries} the histogram regrouped, which only exists above the data nodes.
     */
    public void testUnsetOverARegroupedTimeSeriesRunsAboveTheDataNodes() {
        String histogram = "histogram_quantile(0.5, rate(bucket[10m]))";
        try (EsqlQueryResponse without = run(request("promql-buckets", "max without (instance) (" + histogram + ")"))) {
            Map<String, Double> expected = byLabels(
                run(request("promql-buckets", "max by (job) (histogram_quantile(0.5, sum by (job, instance, le) (rate(bucket[10m]))))")),
                "job"
            );
            assertThat(byTimeSeries(without, "job"), matchesValues(expected));

            List<String> dataUnsets = unsets(drivers(without, "data"));
            assertThat(dataUnsets, not(empty()));
            dataUnsets.forEach(unset -> assertThat(unset, containsString("dimensions=[le]")));
            List<String> aboveUnsets = new ArrayList<>(unsets(drivers(without, "final")));
            aboveUnsets.addAll(unsets(drivers(without, "node_reduce")));
            assertThat(aboveUnsets, not(empty()));
            aboveUnsets.forEach(unset -> assertThat(unset, containsString("dimensions=[instance]")));
        }
    }

    /** A series in two backing indices, on different shards: one identity, one row per step, the same values. */
    public void testSeriesAcrossBackingIndices() {
        try (EsqlQueryResponse without = run(request("promql-metrics-*", "max without (region) (cpu)"))) {
            Map<String, Double> expected = byLabels(run(request("promql-metrics-*", "max by (cluster, pod) (cpu)")), "cluster", "pod");
            Map<String, Double> actual = byTimeSeries(without, "cluster", "pod");
            assertThat(actual, aMapWithSize(getValuesList(without).size()));
            assertThat(actual, matchesValues(expected));
        }
    }

    private EsqlQueryRequest request(String index, String expression) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(
            "PROMQL index=" + index + " " + RANGE + " result=(" + expression + ")"
        );
        request.profile(true);
        if (canUseQueryPragmas()) {
            request.pragmas(
                new QueryPragmas(
                    Settings.builder()
                        .put(QueryPragmas.TASK_CONCURRENCY.getKey(), between(1, 3))
                        .put(QueryPragmas.DATA_PARTITIONING.getKey(), randomFrom(DataPartitioning.values()))
                        .build()
                )
            );
            request.acceptedPragmaRisks(true);
        }
        return request;
    }

    /** The rows of a {@code by} result, keyed by step and {@code labels}, each its own column; closes the response. */
    private static Map<String, Double> byLabels(EsqlQueryResponse response, String... labels) {
        try (response) {
            List<String> columns = response.columns().stream().map(ColumnInfoImpl::name).toList();
            Map<String, Double> rows = new TreeMap<>();
            for (List<Object> row : getValuesList(response)) {
                StringBuilder key = new StringBuilder(String.valueOf(row.get(columns.indexOf("step"))));
                for (String label : labels) {
                    key.append('|').append(row.get(columns.indexOf(label)));
                }
                rows.put(key.toString(), ((Number) row.get(columns.indexOf("result"))).doubleValue());
            }
            return rows;
        }
    }

    /** The rows of a {@code without} result, keyed by step and {@code labels}, read off each row's {@code _timeseries}. */
    private static Map<String, Double> byTimeSeries(EsqlQueryResponse response, String... labels) {
        List<String> columns = response.columns().stream().map(ColumnInfoImpl::name).toList();
        Map<String, Double> rows = new TreeMap<>();
        for (List<Object> row : getValuesList(response)) {
            String timeseries = row.get(columns.indexOf("_timeseries")).toString();
            Map<String, Object> identity = XContentHelper.convertToMap(JsonXContent.jsonXContent, timeseries, false);
            assertThat("the identity carries exactly the remaining labels", identity.keySet(), equalTo(Set.of(labels)));
            StringBuilder key = new StringBuilder(String.valueOf(row.get(columns.indexOf("step"))));
            for (String label : labels) {
                key.append('|').append(identity.get(label));
            }
            rows.put(key.toString(), ((Number) row.get(columns.indexOf("result"))).doubleValue());
        }
        return rows;
    }

    private static Matcher<Map<String, Double>> matchesValues(Map<String, Double> expected) {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Map<String, Double> actual) {
                if (actual.keySet().equals(expected.keySet()) == false) {
                    return false;
                }
                return expected.entrySet().stream().allMatch(e -> closeTo(e.getValue(), 1e-9).matches(actual.get(e.getKey())));
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("rows ").appendValue(expected);
            }
        };
    }

    private static List<DriverProfile> drivers(EsqlQueryResponse response, String description) {
        return response.profile().drivers().stream().filter(driver -> driver.description().equals(description)).toList();
    }

    private static int indexOf(DriverProfile driver, String operator) {
        List<OperatorStatus> operators = driver.operators();
        for (int i = 0; i < operators.size(); i++) {
            if (operators.get(i).operator().contains(operator)) {
                return i;
            }
        }
        return -1;
    }

    private static List<String> unsets(List<DriverProfile> drivers) {
        return drivers.stream()
            .flatMap(driver -> driver.operators().stream())
            .map(OperatorStatus::operator)
            .filter(operator -> operator.contains("TimeSeriesUnsetEvaluator"))
            .toList();
    }

    /** Below the per-series aggregate, no reader loads the {@code _timeseries} metadata (its source) per document. */
    private static void assertNoTimeSeriesRead(OperatorStatus operator) {
        if (operator.status() instanceof ValuesSourceReaderOperatorStatus reader) {
            for (String field : reader.readersBuilt().keySet()) {
                assertThat(operator.operator(), field.startsWith("_timeseries") || field.startsWith("_source"), equalTo(false));
            }
        }
    }
}
