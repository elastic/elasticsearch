/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TimeSeriesRateIT extends AbstractEsqlIntegTestCase {

    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request) {
        assumeTrue("time series available in snapshot builds only", Build.current().isSnapshot());
        return super.run(request);
    }

    record Doc(String host, String cluster, long timestamp, int requestCount, double cpu) {}

    final List<Doc> docs = new ArrayList<>();

    final Map<String, String> hostToClusters = new HashMap<>();
    final Map<String, Integer> hostToRate = new HashMap<>();
    final Map<String, Integer> hostToCpu = new HashMap<>();

    static final float DEVIATION_LIMIT = 0.30f;
    // extra deviation tolerance for subgroups due to fewer samples
    // at 0.35 deviation limit, we see 2/8000 failures. I am expanding to 0.4
    static final float SUBGROUP_DEVIATION_LIMIT = 0.45f;
    // We expect a drop in the rate due to not covering window edges and not triggering
    // extrapolation logic in the time series engine.
    static final float EXPECTED_DROP_RATE = 0.15f;
    static final int LIMIT = 5;
    static final int MAX_HOSTS = 5;
    static final int PCT_CHANCE_OF_RESET = 15; // 15% chance of resetting the request count

    @Before
    public void populateIndex() {
        // this can be expensive, do one
        Settings settings = Settings.builder().put("mode", "time_series").putList("routing_path", List.of("host", "cluster")).build();
        client().admin()
            .indices()
            .prepareCreate("hosts")
            .setSettings(settings)
            .setMapping(
                "@timestamp",
                "type=date",
                "host",
                "type=keyword,time_series_dimension=true",
                "cluster",
                "type=keyword,time_series_dimension=true",
                "cpu",
                "type=double,time_series_metric=gauge",
                "request_count",
                "type=integer,time_series_metric=counter"
            )
            .get();
        final Map<String, Integer> requestCounts = new HashMap<>();
        for (int i = 0; i < MAX_HOSTS; i++) {
            hostToClusters.put("p" + i, randomFrom("qa", "prod"));
            hostToRate.put("p" + i, randomIntBetween(10, 50));
            requestCounts.put("p" + i, randomIntBetween(0, 1000));
            hostToCpu.put("p" + i, randomIntBetween(0, 100));
        }
        long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        int numDocs = between(100, 300);
        docs.clear();
        // We want docs to span a 6-minute period, so we need to adapt their spacing accordingly.
        var avgSamplingPeriod = 360.0 / numDocs; // 6 minutes divided by number of docs - then randomized below

        for (int i = 0; i < numDocs; i++) {
            final var tsChange = randomDoubleBetween(avgSamplingPeriod - 1.0, avgSamplingPeriod + 1.0, true);
            timestamp += Math.round(tsChange * 1000);
            // We want a subset of hosts to have docs within a give time point.
            var hosts = Set.copyOf(randomSubsetOf(between(2, hostToClusters.size()), hostToClusters.keySet()));
            for (String host : hostToClusters.keySet()) {
                var requestCount = requestCounts.compute(host, (k, curr) -> {
                    if (randomInt(100) <= PCT_CHANCE_OF_RESET) {
                        return Math.toIntExact(Math.round(hostToRate.get(k) * tsChange));
                    } else {
                        return Math.toIntExact(Math.round((curr == null ? 0 : curr) + hostToRate.get(k) * tsChange));
                    }
                });
                if (hosts.contains(host)) {
                    docs.add(new Doc(host, hostToClusters.get(host), timestamp, requestCount, hostToCpu.get(host)));
                }
            }
        }
        Randomness.shuffle(docs);
        for (Doc doc : docs) {
            client().prepareIndex("hosts")
                .setSource(
                    "@timestamp",
                    doc.timestamp,
                    "host",
                    doc.host,
                    "cluster",
                    doc.cluster,
                    "cpu",
                    doc.cpu,
                    "request_count",
                    doc.requestCount
                )
                .get();
        }
        client().admin().indices().prepareRefresh("hosts").get();

    }

    private String hostTable() {
        StringBuilder sb = new StringBuilder();
        for (String host : hostToClusters.keySet()) {
            var docsForHost = docs.stream().filter(d -> d.host().equals(host)).toList();
            sb.append(host)
                .append(" -> ")
                .append(hostToClusters.get(host))
                .append(", rate=")
                .append(hostToRate.get(host))
                .append(", cpu=")
                .append(hostToCpu.get(host))
                .append(", numDocs=")
                .append(docsForHost.size())
                .append("\n");
        }
        // Now we add total rate and total CPU used:
        sb.append("Total rate: ").append(hostToRate.values().stream().mapToInt(a -> a).sum()).append("\n");
        sb.append("Average rate: ").append(hostToRate.values().stream().mapToInt(a -> a).average().orElseThrow()).append("\n");
        sb.append("Total CPU: ").append(hostToCpu.values().stream().mapToInt(a -> a).sum()).append("\n");
        sb.append("Average CPU: ").append(hostToCpu.values().stream().mapToInt(a -> a).average().orElseThrow()).append("\n");
        // Add global info
        sb.append("Count of docs: ").append(docs.size()).append("\n");
        // Add docs per minute
        sb.append("Docs in each minute:\n");
        Map<Long, Integer> docsPerMinute = new HashMap<>();
        for (Doc doc : docs) {
            long minute = (doc.timestamp / 60000) % 1000; // convert to minutes
            docsPerMinute.merge(minute, 1, Integer::sum);
        }
        for (Map.Entry<Long, Integer> entry : docsPerMinute.entrySet()) {
            sb.append("Minute ").append(entry.getKey()).append(": ").append(entry.getValue()).append(" docs\n");
        }
        return sb.toString();
    }

    private String valuesTable(List<List<Object>> values) {
        StringBuilder sb = new StringBuilder();
        for (List<Object> row : values) {
            sb.append(row).append("\n");
        }
        return sb.toString();
    }

    public void testRateWithTimeBucketSumByMin() {
        try (
            var resp = run(
                "TS hosts | STATS sum(rate(request_count)) BY tbucket=bucket(@timestamp, 1 minute) | SORT tbucket | LIMIT " + LIMIT
            )
        ) {
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            try {
                assertThat(
                    resp.columns(),
                    equalTo(
                        List.of(new ColumnInfoImpl("sum(rate(request_count))", "double", null), new ColumnInfoImpl("tbucket", "date", null))
                    )
                );
                assertThat(values, hasSize(LIMIT));
                for (int i = 0; i < values.size(); i++) {
                    List<Object> row = values.get(i);
                    assertThat(row, hasSize(2));
                    var totalRate = hostToRate.values().stream().mapToDouble(a -> a + 0.0).sum();
                    assertThat((double) row.get(0), closeTo(totalRate * (1 - EXPECTED_DROP_RATE), DEVIATION_LIMIT * totalRate));
                }
            } catch (AssertionError e) {
                throw new AssertionError("Values:\n" + valuesTable(values) + "\n Hosts:\n" + hostTable(), e);
            }
        }
    }

    public void testRateWithTimeBucketAvgByMin() {
        try (var resp = run("TS hosts | STATS avg(rate(request_count)) BY tbucket=bucket(@timestamp, 1minute) | SORT tbucket | LIMIT 5")) {
            try {
                assertThat(
                    resp.columns(),
                    equalTo(
                        List.of(new ColumnInfoImpl("avg(rate(request_count))", "double", null), new ColumnInfoImpl("tbucket", "date", null))
                    )
                );
                List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
                assertThat(values, hasSize(LIMIT));
                for (int i = 0; i < values.size(); i++) {
                    List<Object> row = values.get(i);
                    assertThat(row, hasSize(2));
                    var expectedRate = hostToRate.values().stream().mapToDouble(a -> a + 0.0).sum() / hostToRate.size();
                    assertThat((double) row.get(0), closeTo(expectedRate * (1 - EXPECTED_DROP_RATE), DEVIATION_LIMIT * expectedRate));
                }
            } catch (AssertionError e) {
                throw new AssertionError("Values:\n" + valuesTable(EsqlTestUtils.getValuesList(resp)) + "\n Hosts:\n" + hostTable(), e);
            }
        }
    }

    public void testRateWithTimeBucketSumByMinAndLimitAsParam() {
        try (var resp = run("""
            TS hosts
            | STATS avg(rate(request_count)) BY tbucket=bucket(@timestamp, 1minute)
            | SORT tbucket
            | LIMIT""" + " " + LIMIT)) {
            try {
                assertThat(
                    resp.columns(),
                    equalTo(
                        List.of(new ColumnInfoImpl("avg(rate(request_count))", "double", null), new ColumnInfoImpl("tbucket", "date", null))
                    )
                );
                List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
                assertThat(values, hasSize(LIMIT));
                for (int i = 0; i < LIMIT; i++) {
                    List<Object> row = values.get(i);
                    assertThat(row, hasSize(2));
                    double expectedAvg = hostToRate.values().stream().mapToDouble(d -> d).sum() / hostToRate.size();
                    assertThat((double) row.get(0), closeTo(expectedAvg * (1 - EXPECTED_DROP_RATE), DEVIATION_LIMIT * expectedAvg));
                }
            } catch (AssertionError e) {
                throw new AssertionError("Values:\n" + valuesTable(EsqlTestUtils.getValuesList(resp)) + "\n Hosts:\n" + hostTable(), e);
            }
        }
    }

    public void testRateWithTimeBucketAndClusterSumByMin() {
        try (var resp = run("""
            TS hosts
            | STATS sum(rate(request_count)) BY tbucket=bucket(@timestamp, 1 minute), cluster
            | SORT tbucket, cluster
            | LIMIT 5""")) {
            try {
                assertThat(
                    resp.columns(),
                    equalTo(
                        List.of(
                            new ColumnInfoImpl("sum(rate(request_count))", "double", null),
                            new ColumnInfoImpl("tbucket", "date", null),
                            new ColumnInfoImpl("cluster", "keyword", null)
                        )
                    )
                );
                List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
                // we have 2 clusters, so we expect 2 * limit rows
                for (List<Object> row : values) {
                    assertThat(row, hasSize(3));
                    String cluster = (String) row.get(2);
                    var expectedRate = hostToClusters.entrySet()
                        .stream()
                        .filter(e -> e.getValue().equals(cluster))
                        .mapToDouble(e -> hostToRate.get(e.getKey()) + 0.0)
                        .sum();
                    assertThat(
                        (double) row.get(0),
                        closeTo(expectedRate * (1 - EXPECTED_DROP_RATE), SUBGROUP_DEVIATION_LIMIT * expectedRate)
                    );
                }
            } catch (AssertionError e) {
                throw new AssertionError("Values:\n" + valuesTable(EsqlTestUtils.getValuesList(resp)) + "\n Hosts:\n" + hostTable(), e);
            }
        }
    }

    public void testRateWithTimeBucketAndClusterAvgByMin() {
        try (var resp = run("""
            TS hosts
            | STATS avg(rate(request_count)) BY tbucket=bucket(@timestamp, 1minute), cluster
            | SORT tbucket, cluster
            | LIMIT 5""")) {
            try {
                assertThat(
                    resp.columns(),
                    equalTo(
                        List.of(
                            new ColumnInfoImpl("avg(rate(request_count))", "double", null),
                            new ColumnInfoImpl("tbucket", "date", null),
                            new ColumnInfoImpl("cluster", "keyword", null)
                        )
                    )
                );
                List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
                for (List<Object> row : values) {
                    assertThat(row, hasSize(3));
                    String cluster = (String) row.get(2);
                    var expectedAvg = hostToClusters.entrySet()
                        .stream()
                        .filter(e -> e.getValue().equals(cluster))
                        .mapToDouble(e -> hostToRate.get(e.getKey()) + 0.0)
                        .average()
                        .orElseThrow();
                    assertThat(
                        (double) row.get(0),
                        closeTo(expectedAvg * (1 - EXPECTED_DROP_RATE), SUBGROUP_DEVIATION_LIMIT * expectedAvg)
                    );
                }
            } catch (AssertionError e) {
                throw new AssertionError("Values:\n" + valuesTable(EsqlTestUtils.getValuesList(resp)) + "\n Hosts:\n" + hostTable(), e);
            }
        }
    }

    public void testRateWithTimeBucketAndClusterMultipleStatsByMin() {
        try (var resp = run("""
            TS hosts
            | STATS
                    s = sum(rate(request_count)),
                    c = count(rate(request_count)),
                    max(rate(request_count)),
                    avg(rate(request_count))
            BY tbucket=bucket(@timestamp, 1minute), cluster
            | SORT tbucket, cluster
            | LIMIT 5
            | EVAL avg_rate= s/c
            | KEEP avg_rate, `max(rate(request_count))`, `avg(rate(request_count))`, tbucket, cluster
            """)) {
            try {
                assertThat(
                    resp.columns(),
                    equalTo(
                        List.of(
                            new ColumnInfoImpl("avg_rate", "double", null),
                            new ColumnInfoImpl("max(rate(request_count))", "double", null),
                            new ColumnInfoImpl("avg(rate(request_count))", "double", null),
                            new ColumnInfoImpl("tbucket", "date", null),
                            new ColumnInfoImpl("cluster", "keyword", null)
                        )
                    )
                );
                List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
                for (List<Object> row : values) {
                    assertThat(row, hasSize(5));
                    String cluster = (String) row.get(4);
                    var expectedAvg = hostToClusters.entrySet()
                        .stream()
                        .filter(e -> e.getValue().equals(cluster))
                        .mapToDouble(e -> hostToRate.get(e.getKey()) + 0.0)
                        .average()
                        .orElseThrow();
                    var expectedMax = hostToClusters.entrySet()
                        .stream()
                        .filter(e -> e.getValue().equals(cluster))
                        .mapToDouble(e -> hostToRate.get(e.getKey()) + 0.0)
                        .max()
                        .orElseThrow();
                    assertThat(
                        (double) row.get(0),
                        closeTo(expectedAvg * (1 - EXPECTED_DROP_RATE), SUBGROUP_DEVIATION_LIMIT * expectedAvg)
                    );
                    assertThat(
                        (double) row.get(2),
                        closeTo(expectedAvg * (1 - EXPECTED_DROP_RATE), SUBGROUP_DEVIATION_LIMIT * expectedAvg)
                    );
                    assertThat(
                        (double) row.get(1),
                        closeTo(expectedMax * (1 - EXPECTED_DROP_RATE), SUBGROUP_DEVIATION_LIMIT * expectedMax)
                    );
                }
            } catch (AssertionError e) {
                throw new AssertionError("Values:\n" + valuesTable(EsqlTestUtils.getValuesList(resp)) + "\n Hosts:\n" + hostTable(), e);
            }
        }
    }

    public void testRateWithTimeBucketAndClusterMultipleMetricsByMin() {
        try (var resp = run("""
            TS hosts
            | STATS sum(rate(request_count)), max(cpu) BY tbucket=bucket(@timestamp, 1 minute), cluster
            | SORT tbucket, cluster
            | LIMIT 5""")) {
            try {
                assertThat(
                    resp.columns(),
                    equalTo(
                        List.of(
                            new ColumnInfoImpl("sum(rate(request_count))", "double", null),
                            new ColumnInfoImpl("max(cpu)", "double", null),
                            new ColumnInfoImpl("tbucket", "date", null),
                            new ColumnInfoImpl("cluster", "keyword", null)
                        )
                    )
                );
                List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
                for (List<Object> row : values) {
                    assertThat(row, hasSize(4));
                    String cluster = (String) row.get(3);
                    var expectedRate = hostToClusters.entrySet()
                        .stream()
                        .filter(e -> e.getValue().equals(cluster))
                        .mapToDouble(e -> hostToRate.get(e.getKey()) + 0.0)
                        .sum();
                    assertThat(
                        (double) row.get(0),
                        closeTo(expectedRate * (1 - EXPECTED_DROP_RATE), SUBGROUP_DEVIATION_LIMIT * expectedRate)
                    );
                    var expectedCpu = hostToClusters.entrySet()
                        .stream()
                        .filter(e -> e.getValue().equals(cluster))
                        .mapToDouble(e -> hostToCpu.get(e.getKey()) + 0.0)
                        .max()
                        .orElseThrow();
                    assertThat(
                        (double) row.get(1),
                        closeTo(expectedCpu * (1 - EXPECTED_DROP_RATE), SUBGROUP_DEVIATION_LIMIT * expectedCpu)
                    );
                }
            } catch (AssertionError e) {
                throw new AssertionError("Values:\n" + valuesTable(EsqlTestUtils.getValuesList(resp)) + "\n Hosts:\n" + hostTable(), e);
            }
        }
    }

    public void testRateWithTimeBucketAndClusterMultipleMetricsAvgByMin() {
        try (var resp = run("""
            TS hosts
            | STATS sum(rate(request_count)), avg(cpu) BY tbucket=bucket(@timestamp, 1 minute), cluster
            | SORT tbucket, cluster
            | LIMIT 5""")) {
            try {
                assertThat(
                    resp.columns(),
                    equalTo(
                        List.of(
                            new ColumnInfoImpl("sum(rate(request_count))", "double", null),
                            new ColumnInfoImpl("avg(cpu)", "double", null),
                            new ColumnInfoImpl("tbucket", "date", null),
                            new ColumnInfoImpl("cluster", "keyword", null)
                        )
                    )
                );
                List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
                for (List<Object> row : values) {
                    assertThat(row, hasSize(4));
                    String cluster = (String) row.get(3);
                    var expectedRate = hostToClusters.entrySet()
                        .stream()
                        .filter(e -> e.getValue().equals(cluster))
                        .mapToDouble(e -> hostToRate.get(e.getKey()) + 0.0)
                        .sum();
                    assertThat(
                        (double) row.get(0),
                        closeTo(expectedRate * (1 - EXPECTED_DROP_RATE), SUBGROUP_DEVIATION_LIMIT * expectedRate)
                    );
                    var expectedCpu = hostToClusters.entrySet()
                        .stream()
                        .filter(e -> e.getValue().equals(cluster))
                        .mapToDouble(e -> hostToCpu.get(e.getKey()) + 0.0)
                        .average()
                        .orElseThrow();
                    assertThat((double) row.get(1), closeTo(expectedCpu, SUBGROUP_DEVIATION_LIMIT * expectedCpu));
                }
            } catch (AssertionError e) {
                throw new AssertionError("Values:\n" + valuesTable(EsqlTestUtils.getValuesList(resp)) + "\n Hosts:\n" + hostTable(), e);
            }
        }
    }
}
