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
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TimeSeriesIT extends AbstractEsqlIntegTestCase {

    @Override
    protected EsqlQueryResponse run(EsqlQueryRequest request) {
        assumeTrue("time series available in snapshot builds only", Build.current().isSnapshot());
        return super.run(request);
    }

    public void testEmpty() {
        Settings settings = Settings.builder().put("mode", "time_series").putList("routing_path", List.of("pod")).build();
        client().admin()
            .indices()
            .prepareCreate("empty_index")
            .setSettings(settings)
            .setMapping(
                "@timestamp",
                "type=date",
                "pod",
                "type=keyword,time_series_dimension=true",
                "cpu",
                "type=long,time_series_metric=gauge"
            )
            .get();
        run("METRICS empty_index | LIMIT 1").close();
    }

    record Doc(String pod, long timestamp, double cpu) {}

    final List<Doc> docs = new ArrayList<>();

    @Before
    public void populateIndex() {
        Settings settings = Settings.builder().put("mode", "time_series").putList("routing_path", List.of("pod")).build();
        client().admin()
            .indices()
            .prepareCreate("pods")
            .setSettings(settings)
            .setMapping(
                "@timestamp",
                "type=date",
                "pod",
                "type=keyword,time_series_dimension=true",
                "cpu",
                "type=double,time_series_metric=gauge"
            )
            .get();
        List<String> allPods = IntStream.rangeClosed(1, 9).mapToObj(n -> "p" + n).toList();
        long timestamp = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        int numDocs = between(10, 500);
        docs.clear();
        for (int i = 0; i < numDocs; i++) {
            List<String> pods = randomSubsetOf(between(1, allPods.size()), allPods);
            timestamp += between(1, 10) * 1000L;
            for (String pod : pods) {
                int cpu = randomIntBetween(0, 100);
                docs.add(new Doc(pod, timestamp, cpu));
            }
        }
        Randomness.shuffle(docs);
        for (Doc doc : docs) {
            client().prepareIndex("pods").setSource("@timestamp", doc.timestamp, "pod", doc.pod, "cpu", doc.cpu).get();
        }
    }

    public void testSimpleMetrics() {
        List<String> sortedGroups = docs.stream().map(d -> d.pod).distinct().sorted().toList();
        client().admin().indices().prepareRefresh("pods").get();
        try (EsqlQueryResponse resp = run("METRICS pods load=avg(cpu) BY pod | SORT pod")) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            assertThat(rows, hasSize(sortedGroups.size()));
            for (int i = 0; i < rows.size(); i++) {
                List<Object> r = rows.get(i);
                String pod = (String) r.get(1);
                assertThat(pod, equalTo(sortedGroups.get(i)));
                List<Double> values = docs.stream().filter(d -> d.pod.equals(pod)).map(d -> d.cpu).toList();
                double avg = values.stream().mapToDouble(n -> n).sum() / values.size();
                assertThat((double) r.get(0), equalTo(avg));
            }
        }
        try (EsqlQueryResponse resp = run("METRICS pods | SORT @timestamp DESC, pod | KEEP @timestamp, pod, cpu | LIMIT 5")) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            List<Doc> topDocs = docs.stream()
                .sorted(Comparator.comparingLong(Doc::timestamp).reversed().thenComparing(Doc::pod))
                .limit(5)
                .toList();
            assertThat(rows, hasSize(topDocs.size()));
            for (int i = 0; i < rows.size(); i++) {
                List<Object> r = rows.get(i);
                long timestamp = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis((String) r.get(0));
                String pod = (String) r.get(1);
                double cpu = (Double) r.get(2);
                assertThat(topDocs.get(i).timestamp, equalTo(timestamp));
                assertThat(topDocs.get(i).pod, equalTo(pod));
                assertThat(topDocs.get(i).cpu, equalTo(cpu));
            }
        }
    }

    public void testTimeBucket() {
        record Key(String pod, long interval) {

        }
        var rounding = Rounding.builder(TimeValue.timeValueSeconds(60)).build().prepare(0, Long.MAX_VALUE);
        Map<Key, List<Double>> groups = new HashMap<>();
        for (Doc doc : docs) {
            Key key = new Key(doc.pod, rounding.round(doc.timestamp));
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(doc.cpu);
        }
        client().admin().indices().prepareRefresh("pods").get();
        try (EsqlQueryResponse resp = run("METRICS pods load=avg(cpu) BY pod,bucket=tbucket(1minute) | SORT bucket DESC, pod")) {
            List<Key> sortedGroups = groups.keySet()
                .stream()
                .sorted(Comparator.comparingLong(Key::interval).reversed().thenComparing(Key::pod))
                .toList();
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            assertThat(rows, hasSize(sortedGroups.size()));
            for (int i = 0; i < rows.size(); i++) {
                Key key = sortedGroups.get(i);
                List<Object> r = rows.get(i);
                double avgLoad = (double) r.get(0);
                String pod = (String) r.get(1);
                long interval = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis((String) r.get(2));

                assertThat(pod, equalTo(key.pod));
                assertThat(interval, equalTo(key.interval));
                List<Double> values = groups.get(key);
                double expectedAvg = values.stream().mapToDouble(n -> n).sum() / values.size();
                assertThat(avgLoad, equalTo(expectedAvg));
            }
        }
    }
}
