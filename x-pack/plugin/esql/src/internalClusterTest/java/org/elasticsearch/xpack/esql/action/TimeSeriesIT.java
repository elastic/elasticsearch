/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.esql.EsqlTestUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
            .prepareCreate("pods")
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
        run("METRICS pods | LIMIT 1").close();
    }

    public void testSimpleMetrics() {
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
        List<String> pods = List.of("p1", "p2", "p3");
        long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        int numDocs = between(10, 100);
        record Doc(String pod, long timestamp, double cpu) {}
        List<Doc> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String pod = randomFrom(pods);
            int cpu = randomIntBetween(0, 100);
            long timestamp = startTime + (1000L * i);
            docs.add(new Doc(pod, timestamp, cpu));
            client().prepareIndex("pods").setSource("@timestamp", timestamp, "pod", pod, "cpu", cpu).get();
        }
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
        try (EsqlQueryResponse resp = run("METRICS pods | SORT @timestamp DESC | KEEP @timestamp, pod, cpu | LIMIT 5")) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            List<Doc> topDocs = docs.stream().sorted(Comparator.comparingLong(Doc::timestamp).reversed()).limit(5).toList();
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
}
