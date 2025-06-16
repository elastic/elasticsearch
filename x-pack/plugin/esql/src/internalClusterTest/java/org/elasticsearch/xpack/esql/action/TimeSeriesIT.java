/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.Before;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TimeSeriesIT extends AbstractEsqlIntegTestCase {

    @Override
    protected EsqlQueryResponse run(EsqlQueryRequest request) {
        assumeTrue("time series available in snapshot builds only", Build.current().isSnapshot());
        return super.run(request);
    }

    public void testEmpty() {
        Settings settings = Settings.builder().put("mode", "time_series").putList("routing_path", List.of("host")).build();
        client().admin()
            .indices()
            .prepareCreate("empty_index")
            .setSettings(settings)
            .setMapping(
                "@timestamp",
                "type=date",
                "host",
                "type=keyword,time_series_dimension=true",
                "cpu",
                "type=long,time_series_metric=gauge"
            )
            .get();
        run("METRICS empty_index | LIMIT 1").close();
    }

    record Doc(String host, String cluster, long timestamp, int requestCount, double cpu) {}

    final List<Doc> docs = new ArrayList<>();

    record RequestCounter(long timestamp, long count) {

    }

    static Double computeRate(List<RequestCounter> values) {
        List<RequestCounter> sorted = values.stream().sorted(Comparator.comparingLong(RequestCounter::timestamp)).toList();
        if (sorted.size() < 2) {
            return null;
        }
        long resets = 0;
        for (int i = 0; i < sorted.size() - 1; i++) {
            if (sorted.get(i).count > sorted.get(i + 1).count) {
                resets += sorted.get(i).count;
            }
        }
        RequestCounter last = sorted.get(sorted.size() - 1);
        RequestCounter first = sorted.get(0);
        double dv = resets + last.count - first.count;
        double dt = last.timestamp - first.timestamp;
        return dv * 1000 / dt;
    }

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
        Map<String, String> hostToClusters = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            hostToClusters.put("p" + i, randomFrom("qa", "prod"));
        }
        long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        int numDocs = between(20, 100);
        docs.clear();
        Map<String, Integer> requestCounts = new HashMap<>();
        for (int i = 0; i < numDocs; i++) {
            List<String> hosts = randomSubsetOf(between(1, hostToClusters.size()), hostToClusters.keySet());
            timestamp += between(1, 10) * 1000L;
            for (String host : hosts) {
                var requestCount = requestCounts.compute(host, (k, curr) -> {
                    if (curr == null || randomInt(100) <= 20) {
                        return randomIntBetween(0, 10);
                    } else {
                        return curr + randomIntBetween(1, 10);
                    }
                });
                int cpu = randomIntBetween(0, 100);
                docs.add(new Doc(host, hostToClusters.get(host), timestamp, requestCount, cpu));
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

    public void testSimpleMetrics() {
        List<String> sortedGroups = docs.stream().map(d -> d.host).distinct().sorted().toList();
        client().admin().indices().prepareRefresh("hosts").get();
        try (EsqlQueryResponse resp = run("METRICS hosts load=avg(cpu) BY host | SORT host")) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            assertThat(rows, hasSize(sortedGroups.size()));
            for (int i = 0; i < rows.size(); i++) {
                List<Object> r = rows.get(i);
                String pod = (String) r.get(1);
                assertThat(pod, equalTo(sortedGroups.get(i)));
                List<Double> values = docs.stream().filter(d -> d.host.equals(pod)).map(d -> d.cpu).toList();
                double avg = values.stream().mapToDouble(n -> n).sum() / values.size();
                assertThat((double) r.get(0), equalTo(avg));
            }
        }
        try (EsqlQueryResponse resp = run("METRICS hosts | SORT @timestamp DESC, host | KEEP @timestamp, host, cpu | LIMIT 5")) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            List<Doc> topDocs = docs.stream()
                .sorted(Comparator.comparingLong(Doc::timestamp).reversed().thenComparing(Doc::host))
                .limit(5)
                .toList();
            assertThat(rows, hasSize(topDocs.size()));
            for (int i = 0; i < rows.size(); i++) {
                List<Object> r = rows.get(i);
                long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis((String) r.get(0));
                String pod = (String) r.get(1);
                double cpu = (Double) r.get(2);
                assertThat(topDocs.get(i).timestamp, equalTo(timestamp));
                assertThat(topDocs.get(i).host, equalTo(pod));
                assertThat(topDocs.get(i).cpu, equalTo(cpu));
            }
        }
    }

    public void testRateWithoutGrouping() {
        record RateKey(String cluster, String host) {

        }
        Map<RateKey, List<RequestCounter>> groups = new HashMap<>();
        for (Doc doc : docs) {
            RateKey key = new RateKey(doc.cluster, doc.host);
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(new RequestCounter(doc.timestamp, doc.requestCount));
        }
        List<Double> rates = new ArrayList<>();
        for (List<RequestCounter> group : groups.values()) {
            Double v = computeRate(group);
            if (v != null) {
                rates.add(v);
            }
        }
        try (var resp = run("METRICS hosts sum(rate(request_count, 1second))")) {
            assertThat(resp.columns(), equalTo(List.of(new ColumnInfoImpl("sum(rate(request_count, 1second))", "double", null))));
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), hasSize(1));
            assertThat((double) values.get(0).get(0), closeTo(rates.stream().mapToDouble(d -> d).sum(), 0.1));
        }
        try (var resp = run("METRICS hosts max(rate(request_count)), min(rate(request_count))")) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("max(rate(request_count))", "double", null),
                        new ColumnInfoImpl("min(rate(request_count))", "double", null)
                    )
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), hasSize(2));
            assertThat((double) values.get(0).get(0), closeTo(rates.stream().mapToDouble(d -> d).max().orElse(0.0), 0.1));
            assertThat((double) values.get(0).get(1), closeTo(rates.stream().mapToDouble(d -> d).min().orElse(0.0), 0.1));
        }
        try (var resp = run("METRICS hosts max(rate(request_count)), avg(rate(request_count)), max(rate(request_count, 1minute))")) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("max(rate(request_count))", "double", null),
                        new ColumnInfoImpl("avg(rate(request_count))", "double", null),
                        new ColumnInfoImpl("max(rate(request_count, 1minute))", "double", null)
                    )
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), hasSize(3));
            assertThat((double) values.get(0).get(0), closeTo(rates.stream().mapToDouble(d -> d).max().orElse(0.0), 0.1));
            final double avg = rates.isEmpty() ? 0.0 : rates.stream().mapToDouble(d -> d).sum() / rates.size();
            assertThat((double) values.get(0).get(1), closeTo(avg, 0.1));
            assertThat((double) values.get(0).get(2), closeTo(rates.stream().mapToDouble(d -> d * 60.0).max().orElse(0.0), 0.1));
        }
        try (var resp = run("METRICS hosts avg(rate(request_count)), avg(rate(request_count, 1second))")) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("avg(rate(request_count))", "double", null),
                        new ColumnInfoImpl("avg(rate(request_count, 1second))", "double", null)
                    )
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), hasSize(2));
            final double avg = rates.isEmpty() ? 0.0 : rates.stream().mapToDouble(d -> d).sum() / rates.size();
            assertThat((double) values.get(0).get(0), closeTo(avg, 0.1));
            assertThat((double) values.get(0).get(1), closeTo(avg, 0.1));
        }
        try (var resp = run("METRICS hosts max(rate(request_count)), min(rate(request_count)), min(cpu), max(cpu)")) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("max(rate(request_count))", "double", null),
                        new ColumnInfoImpl("min(rate(request_count))", "double", null),
                        new ColumnInfoImpl("min(cpu)", "double", null),
                        new ColumnInfoImpl("max(cpu)", "double", null)
                    )
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), hasSize(4));
            assertThat((double) values.get(0).get(0), closeTo(rates.stream().mapToDouble(d -> d).max().orElse(0.0), 0.1));
            assertThat((double) values.get(0).get(1), closeTo(rates.stream().mapToDouble(d -> d).min().orElse(0.0), 0.1));
            double minCpu = docs.stream().mapToDouble(d -> d.cpu).min().orElse(Long.MAX_VALUE);
            double maxCpu = docs.stream().mapToDouble(d -> d.cpu).max().orElse(Long.MIN_VALUE);
            assertThat((double) values.get(0).get(2), closeTo(minCpu, 0.1));
            assertThat((double) values.get(0).get(3), closeTo(maxCpu, 0.1));
        }
    }

    public void testRateGroupedByCluster() {
        record RateKey(String cluster, String host) {

        }
        Map<RateKey, List<RequestCounter>> groups = new HashMap<>();
        for (Doc doc : docs) {
            RateKey key = new RateKey(doc.cluster, doc.host);
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(new RequestCounter(doc.timestamp, doc.requestCount));
        }
        Map<String, List<Double>> bucketToRates = new HashMap<>();
        for (Map.Entry<RateKey, List<RequestCounter>> e : groups.entrySet()) {
            List<Double> values = bucketToRates.computeIfAbsent(e.getKey().cluster, k -> new ArrayList<>());
            Double rate = computeRate(e.getValue());
            values.add(Objects.requireNonNullElse(rate, 0.0));
        }
        List<String> sortedKeys = bucketToRates.keySet().stream().sorted().toList();
        try (var resp = run("METRICS hosts sum(rate(request_count)) BY cluster | SORT cluster")) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(new ColumnInfoImpl("sum(rate(request_count))", "double", null), new ColumnInfoImpl("cluster", "keyword", null))
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(bucketToRates.size()));
            for (int i = 0; i < bucketToRates.size(); i++) {
                List<Object> row = values.get(i);
                assertThat(row, hasSize(2));
                String key = sortedKeys.get(i);
                assertThat(row.get(1), equalTo(key));
                assertThat((double) row.get(0), closeTo(bucketToRates.get(key).stream().mapToDouble(d -> d).sum(), 0.1));
            }
        }
        try (var resp = run("METRICS hosts avg(rate(request_count)) BY cluster | SORT cluster")) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(new ColumnInfoImpl("avg(rate(request_count))", "double", null), new ColumnInfoImpl("cluster", "keyword", null))
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(bucketToRates.size()));
            for (int i = 0; i < bucketToRates.size(); i++) {
                List<Object> row = values.get(i);
                assertThat(row, hasSize(2));
                String key = sortedKeys.get(i);
                assertThat(row.get(1), equalTo(key));
                List<Double> rates = bucketToRates.get(key);
                if (rates.isEmpty()) {
                    assertThat(row.get(0), equalTo(0.0));
                } else {
                    double avg = rates.stream().mapToDouble(d -> d).sum() / rates.size();
                    assertThat((double) row.get(0), closeTo(avg, 0.1));
                }
            }
        }
        try (var resp = run("METRICS hosts avg(rate(request_count, 1minute)), avg(rate(request_count)) BY cluster | SORT cluster")) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("avg(rate(request_count, 1minute))", "double", null),
                        new ColumnInfoImpl("avg(rate(request_count))", "double", null),
                        new ColumnInfoImpl("cluster", "keyword", null)
                    )
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(bucketToRates.size()));
            for (int i = 0; i < bucketToRates.size(); i++) {
                List<Object> row = values.get(i);
                assertThat(row, hasSize(3));
                String key = sortedKeys.get(i);
                assertThat(row.get(2), equalTo(key));
                List<Double> rates = bucketToRates.get(key);
                if (rates.isEmpty()) {
                    assertThat(row.get(0), equalTo(0.0));
                    assertThat(row.get(1), equalTo(0.0));
                } else {
                    double avg = rates.stream().mapToDouble(d -> d).sum() / rates.size();
                    assertThat((double) row.get(0), closeTo(avg * 60.0f, 0.1));
                    assertThat((double) row.get(1), closeTo(avg, 0.1));
                }
            }
        }
    }

    public void testRateWithTimeBucket() {
        var rounding = new Rounding.Builder(TimeValue.timeValueSeconds(60)).timeZone(ZoneOffset.UTC).build().prepareForUnknown();
        record RateKey(String host, String cluster, long interval) {}
        Map<RateKey, List<RequestCounter>> groups = new HashMap<>();
        for (Doc doc : docs) {
            RateKey key = new RateKey(doc.host, doc.cluster, rounding.round(doc.timestamp));
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(new RequestCounter(doc.timestamp, doc.requestCount));
        }
        Map<Long, List<Double>> bucketToRates = new HashMap<>();
        for (Map.Entry<RateKey, List<RequestCounter>> e : groups.entrySet()) {
            List<Double> values = bucketToRates.computeIfAbsent(e.getKey().interval, k -> new ArrayList<>());
            Double rate = computeRate(e.getValue());
            if (rate != null) {
                values.add(rate);
            }
        }
        List<Long> sortedKeys = bucketToRates.keySet().stream().sorted().limit(5).toList();
        try (var resp = run("METRICS hosts sum(rate(request_count)) BY ts=bucket(@timestamp, 1 minute) | SORT ts | LIMIT 5")) {
            assertThat(
                resp.columns(),
                equalTo(List.of(new ColumnInfoImpl("sum(rate(request_count))", "double", null), new ColumnInfoImpl("ts", "date", null)))
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(sortedKeys.size()));
            for (int i = 0; i < sortedKeys.size(); i++) {
                List<Object> row = values.get(i);
                assertThat(row, hasSize(2));
                long key = sortedKeys.get(i);
                assertThat(row.get(1), equalTo(DEFAULT_DATE_TIME_FORMATTER.formatMillis(key)));
                List<Double> bucketValues = bucketToRates.get(key);
                if (bucketValues.isEmpty()) {
                    assertNull(row.get(0));
                } else {
                    assertThat((double) row.get(0), closeTo(bucketValues.stream().mapToDouble(d -> d).sum(), 0.1));
                }
            }
        }
        try (var resp = run("METRICS hosts avg(rate(request_count)) BY ts=bucket(@timestamp, 1minute) | SORT ts | LIMIT 5")) {
            assertThat(
                resp.columns(),
                equalTo(List.of(new ColumnInfoImpl("avg(rate(request_count))", "double", null), new ColumnInfoImpl("ts", "date", null)))
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(sortedKeys.size()));
            for (int i = 0; i < sortedKeys.size(); i++) {
                List<Object> row = values.get(i);
                assertThat(row, hasSize(2));
                long key = sortedKeys.get(i);
                assertThat(row.get(1), equalTo(DEFAULT_DATE_TIME_FORMATTER.formatMillis(key)));
                List<Double> bucketValues = bucketToRates.get(key);
                if (bucketValues.isEmpty()) {
                    assertNull(row.get(0));
                } else {
                    double avg = bucketValues.stream().mapToDouble(d -> d).sum() / bucketValues.size();
                    assertThat((double) row.get(0), closeTo(avg, 0.1));
                }
            }
        }
        try (var resp = run("""
            METRICS hosts avg(rate(request_count, 1minute)), avg(rate(request_count)) BY ts=bucket(@timestamp, 1minute)
            | SORT ts
            | LIMIT 5
            """)) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("avg(rate(request_count, 1minute))", "double", null),
                        new ColumnInfoImpl("avg(rate(request_count))", "double", null),
                        new ColumnInfoImpl("ts", "date", null)
                    )
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(sortedKeys.size()));
            for (int i = 0; i < sortedKeys.size(); i++) {
                List<Object> row = values.get(i);
                assertThat(row, hasSize(3));
                long key = sortedKeys.get(i);
                assertThat(row.get(2), equalTo(DEFAULT_DATE_TIME_FORMATTER.formatMillis(key)));
                List<Double> bucketValues = bucketToRates.get(key);
                if (bucketValues.isEmpty()) {
                    assertNull(row.get(0));
                    assertNull(row.get(1));
                } else {
                    double avg = bucketValues.stream().mapToDouble(d -> d).sum() / bucketValues.size();
                    assertThat((double) row.get(0), closeTo(avg * 60.0f, 0.1));
                    assertThat((double) row.get(1), closeTo(avg, 0.1));
                }
            }
        }
    }

    public void testRateWithTimeBucketAndCluster() {
        var rounding = new Rounding.Builder(TimeValue.timeValueSeconds(60)).timeZone(ZoneOffset.UTC).build().prepareForUnknown();
        record RateKey(String host, String cluster, long interval) {}
        Map<RateKey, List<RequestCounter>> groups = new HashMap<>();
        for (Doc doc : docs) {
            RateKey key = new RateKey(doc.host, doc.cluster, rounding.round(doc.timestamp));
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(new RequestCounter(doc.timestamp, doc.requestCount));
        }
        record GroupKey(String cluster, long interval) {}
        Map<GroupKey, List<Double>> rateBuckets = new HashMap<>();
        for (Map.Entry<RateKey, List<RequestCounter>> e : groups.entrySet()) {
            RateKey key = e.getKey();
            List<Double> values = rateBuckets.computeIfAbsent(new GroupKey(key.cluster, key.interval), k -> new ArrayList<>());
            Double rate = computeRate(e.getValue());
            if (rate != null) {
                values.add(rate);
            }
        }
        Map<GroupKey, List<Double>> cpuBuckets = new HashMap<>();
        for (Doc doc : docs) {
            GroupKey key = new GroupKey(doc.cluster, rounding.round(doc.timestamp));
            cpuBuckets.computeIfAbsent(key, k -> new ArrayList<>()).add(doc.cpu);
        }
        List<GroupKey> sortedKeys = rateBuckets.keySet()
            .stream()
            .sorted(Comparator.comparing(GroupKey::interval).thenComparing(GroupKey::cluster))
            .limit(5)
            .toList();
        try (var resp = run("""
            METRICS hosts sum(rate(request_count)) BY ts=bucket(@timestamp, 1 minute), cluster
            | SORT ts, cluster
            | LIMIT 5""")) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("sum(rate(request_count))", "double", null),
                        new ColumnInfoImpl("ts", "date", null),
                        new ColumnInfoImpl("cluster", "keyword", null)
                    )
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(sortedKeys.size()));
            for (int i = 0; i < sortedKeys.size(); i++) {
                List<Object> row = values.get(i);
                assertThat(row, hasSize(3));
                var key = sortedKeys.get(i);
                assertThat(row.get(1), equalTo(DEFAULT_DATE_TIME_FORMATTER.formatMillis(key.interval)));
                assertThat(row.get(2), equalTo(key.cluster));
                List<Double> bucketValues = rateBuckets.get(key);
                if (bucketValues.isEmpty()) {
                    assertNull(row.get(0));
                } else {
                    assertThat((double) row.get(0), closeTo(bucketValues.stream().mapToDouble(d -> d).sum(), 0.1));
                }
            }
        }
        try (var resp = run("""
            METRICS hosts avg(rate(request_count)) BY ts=bucket(@timestamp, 1minute), cluster
            | SORT ts, cluster
            | LIMIT 5""")) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("avg(rate(request_count))", "double", null),
                        new ColumnInfoImpl("ts", "date", null),
                        new ColumnInfoImpl("cluster", "keyword", null)
                    )
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(sortedKeys.size()));
            for (int i = 0; i < sortedKeys.size(); i++) {
                List<Object> row = values.get(i);
                assertThat(row, hasSize(3));
                var key = sortedKeys.get(i);
                assertThat(row.get(1), equalTo(DEFAULT_DATE_TIME_FORMATTER.formatMillis(key.interval)));
                assertThat(row.get(2), equalTo(key.cluster));
                List<Double> bucketValues = rateBuckets.get(key);
                if (bucketValues.isEmpty()) {
                    assertNull(row.get(0));
                } else {
                    double avg = bucketValues.stream().mapToDouble(d -> d).sum() / bucketValues.size();
                    assertThat((double) row.get(0), closeTo(avg, 0.1));
                }
            }
        }
        try (var resp = run("""
            METRICS hosts avg(rate(request_count, 1minute)), avg(rate(request_count)) BY ts=bucket(@timestamp, 1minute), cluster
            | SORT ts, cluster
            | LIMIT 5""")) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("avg(rate(request_count, 1minute))", "double", null),
                        new ColumnInfoImpl("avg(rate(request_count))", "double", null),
                        new ColumnInfoImpl("ts", "date", null),
                        new ColumnInfoImpl("cluster", "keyword", null)
                    )
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(sortedKeys.size()));
            for (int i = 0; i < sortedKeys.size(); i++) {
                List<Object> row = values.get(i);
                assertThat(row, hasSize(4));
                var key = sortedKeys.get(i);
                assertThat(row.get(2), equalTo(DEFAULT_DATE_TIME_FORMATTER.formatMillis(key.interval)));
                assertThat(row.get(3), equalTo(key.cluster));
                List<Double> bucketValues = rateBuckets.get(key);
                if (bucketValues.isEmpty()) {
                    assertNull(row.get(0));
                    assertNull(row.get(1));
                } else {
                    double avg = bucketValues.stream().mapToDouble(d -> d).sum() / bucketValues.size();
                    assertThat((double) row.get(0), closeTo(avg * 60.0f, 0.1));
                    assertThat((double) row.get(1), closeTo(avg, 0.1));
                }
            }
        }
        try (var resp = run("""
            METRICS hosts
                    s = sum(rate(request_count)),
                    c = count(rate(request_count)),
                    max(rate(request_count)),
                    avg(rate(request_count))
            BY ts=bucket(@timestamp, 1minute), cluster
            | SORT ts, cluster
            | LIMIT 5
            | EVAL avg_rate= s/c
            | KEEP avg_rate, `max(rate(request_count))`, `avg(rate(request_count))`, ts, cluster
            """)) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("avg_rate", "double", null),
                        new ColumnInfoImpl("max(rate(request_count))", "double", null),
                        new ColumnInfoImpl("avg(rate(request_count))", "double", null),
                        new ColumnInfoImpl("ts", "date", null),
                        new ColumnInfoImpl("cluster", "keyword", null)
                    )
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(sortedKeys.size()));
            for (int i = 0; i < sortedKeys.size(); i++) {
                List<Object> row = values.get(i);
                assertThat(row, hasSize(5));
                var key = sortedKeys.get(i);
                assertThat(row.get(3), equalTo(DEFAULT_DATE_TIME_FORMATTER.formatMillis(key.interval)));
                assertThat(row.get(4), equalTo(key.cluster));
                List<Double> bucketValues = rateBuckets.get(key);
                if (bucketValues.isEmpty()) {
                    assertNull(row.get(0));
                    assertNull(row.get(1));
                } else {
                    double avg = bucketValues.stream().mapToDouble(d -> d).sum() / bucketValues.size();
                    assertThat((double) row.get(0), closeTo(avg, 0.1));
                    double max = bucketValues.stream().mapToDouble(d -> d).max().orElse(0.0);
                    assertThat((double) row.get(1), closeTo(max, 0.1));
                }
                assertEquals(row.get(0), row.get(2));
            }
        }
        try (var resp = run("""
            METRICS hosts sum(rate(request_count)), max(cpu) BY ts=bucket(@timestamp, 1 minute), cluster
            | SORT ts, cluster
            | LIMIT 5""")) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("sum(rate(request_count))", "double", null),
                        new ColumnInfoImpl("max(cpu)", "double", null),
                        new ColumnInfoImpl("ts", "date", null),
                        new ColumnInfoImpl("cluster", "keyword", null)
                    )
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(sortedKeys.size()));
            for (int i = 0; i < sortedKeys.size(); i++) {
                List<Object> row = values.get(i);
                assertThat(row, hasSize(4));
                var key = sortedKeys.get(i);
                assertThat(row.get(2), equalTo(DEFAULT_DATE_TIME_FORMATTER.formatMillis(key.interval)));
                assertThat(row.get(3), equalTo(key.cluster));
                List<Double> rateBucket = rateBuckets.get(key);
                if (rateBucket.isEmpty()) {
                    assertNull(row.get(0));
                } else {
                    assertThat((double) row.get(0), closeTo(rateBucket.stream().mapToDouble(d -> d).sum(), 0.1));
                }
                List<Double> cpuBucket = cpuBuckets.get(key);
                if (cpuBuckets.isEmpty()) {
                    assertNull(row.get(1));
                } else {
                    assertThat((double) row.get(1), closeTo(cpuBucket.stream().mapToDouble(d -> d).max().orElse(0.0), 0.1));
                }
            }
        }
        try (var resp = run("""
            METRICS hosts sum(rate(request_count)), avg(cpu) BY ts=bucket(@timestamp, 1 minute), cluster
            | SORT ts, cluster
            | LIMIT 5""")) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("sum(rate(request_count))", "double", null),
                        new ColumnInfoImpl("avg(cpu)", "double", null),
                        new ColumnInfoImpl("ts", "date", null),
                        new ColumnInfoImpl("cluster", "keyword", null)
                    )
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(sortedKeys.size()));
            for (int i = 0; i < sortedKeys.size(); i++) {
                List<Object> row = values.get(i);
                assertThat(row, hasSize(4));
                var key = sortedKeys.get(i);
                assertThat(row.get(2), equalTo(DEFAULT_DATE_TIME_FORMATTER.formatMillis(key.interval)));
                assertThat(row.get(3), equalTo(key.cluster));
                List<Double> rateBucket = rateBuckets.get(key);
                if (rateBucket.isEmpty()) {
                    assertNull(row.get(0));
                } else {
                    assertThat((double) row.get(0), closeTo(rateBucket.stream().mapToDouble(d -> d).sum(), 0.1));
                }
                List<Double> cpuBucket = cpuBuckets.get(key);
                if (cpuBuckets.isEmpty()) {
                    assertNull(row.get(1));
                } else {
                    double avg = cpuBucket.stream().mapToDouble(d -> d).sum() / cpuBucket.size();
                    assertThat((double) row.get(1), closeTo(avg, 0.1));
                }
            }
        }
    }

    public void testApplyRateBeforeFinalGrouping() {
        record RateKey(String cluster, String host) {

        }
        Map<RateKey, List<RequestCounter>> groups = new HashMap<>();
        for (Doc doc : docs) {
            RateKey key = new RateKey(doc.cluster, doc.host);
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(new RequestCounter(doc.timestamp, doc.requestCount));
        }
        List<Double> rates = new ArrayList<>();
        for (List<RequestCounter> group : groups.values()) {
            Double v = computeRate(group);
            if (v != null) {
                rates.add(v);
            }
        }
        try (var resp = run("METRICS hosts sum(abs(rate(request_count, 1second)))")) {
            assertThat(resp.columns(), equalTo(List.of(new ColumnInfoImpl("sum(abs(rate(request_count, 1second)))", "double", null))));
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), hasSize(1));
            assertThat((double) values.get(0).get(0), closeTo(rates.stream().mapToDouble(d -> d).sum(), 0.1));
        }
        try (var resp = run("METRICS hosts sum(10.0 * rate(request_count, 1second))")) {
            assertThat(resp.columns(), equalTo(List.of(new ColumnInfoImpl("sum(10.0 * rate(request_count, 1second))", "double", null))));
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), hasSize(1));
            assertThat((double) values.get(0).get(0), closeTo(rates.stream().mapToDouble(d -> d * 10.0).sum(), 0.1));
        }
        try (var resp = run("METRICS hosts sum(20 * rate(request_count, 1second) + 10 * floor(rate(request_count, 1second)))")) {
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl(
                            "sum(20 * rate(request_count, 1second) + 10 * floor(rate(request_count, 1second)))",
                            "double",
                            null
                        )
                    )
                )
            );
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), hasSize(1));
            assertThat((double) values.get(0).get(0), closeTo(rates.stream().mapToDouble(d -> 20. * d + 10.0 * Math.floor(d)).sum(), 0.1));
        }
    }

    public void testIndexMode() {
        createIndex("events");
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            index("events", Integer.toString(i), Map.of("v", i));
        }
        refresh("events");
        List<ColumnInfoImpl> columns = List.of(
            new ColumnInfoImpl("_index", DataType.KEYWORD, null),
            new ColumnInfoImpl("_index_mode", DataType.KEYWORD, null)
        );
        try (EsqlQueryResponse resp = run("""
            FROM events,hosts METADATA _index_mode, _index
            | WHERE _index_mode == "time_series"
            | STATS BY _index, _index_mode
            """)) {
            assertThat(resp.columns(), equalTo(columns));
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values, equalTo(List.of(List.of("hosts", "time_series"))));
        }
        try (EsqlQueryResponse resp = run("""
            FROM events,hosts METADATA _index_mode, _index
            | WHERE _index_mode == "standard"
            | STATS BY _index, _index_mode
            """)) {
            assertThat(resp.columns(), equalTo(columns));
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values, equalTo(List.of(List.of("events", "standard"))));
        }
        try (EsqlQueryResponse resp = run("""
            FROM events,hosts METADATA _index_mode, _index
            | STATS BY _index, _index_mode
            | SORT _index
            """)) {
            assertThat(resp.columns(), equalTo(columns));
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            assertThat(values, hasSize(2));
            assertThat(values, equalTo(List.of(List.of("events", "standard"), List.of("hosts", "time_series"))));
        }
    }
}
