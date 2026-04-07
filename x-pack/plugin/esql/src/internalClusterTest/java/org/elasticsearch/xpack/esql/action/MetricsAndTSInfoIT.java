/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

public class MetricsAndTSInfoIT extends AbstractEsqlIntegTestCase {

    record Doc(
        Collection<String> project,
        String host,
        String cluster,
        long timestamp,
        int requestCount,
        double cpu,
        ByteSizeValue memory
    ) {}

    final List<Doc> docs = new ArrayList<>();

    @Before
    public void populateIndices() {
        Settings.Builder settings = Settings.builder().put("mode", "time_series").putList("routing_path", List.of("host", "cluster"));
        if (randomBoolean()) {
            settings.put("index.codec", "default").put("index.number_of_replicas", 0).put(IndexSettings.SYNTHETIC_ID.getKey(), true);
        }
        {
            client().admin()
                .indices()
                .prepareCreate("index-1")
                .setSettings(settings)
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "project",
                    "type=keyword",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "cluster",
                    "type=keyword,time_series_dimension=true",
                    "cpu",
                    "type=double,time_series_metric=gauge",
                    "memory",
                    "type=long,time_series_metric=gauge",
                    "request_count",
                    "type=integer,time_series_metric=counter",
                    "extra-field-1",
                    "type=keyword"
                )
                .get();
        }
        {
            client().admin()
                .indices()
                .prepareCreate("index-2")
                .setSettings(settings)
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "project",
                    "type=keyword",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "cluster",
                    "type=keyword,time_series_dimension=true",
                    "cpu",
                    "type=double,time_series_metric=gauge",
                    "memory",
                    "type=long,time_series_metric=gauge",
                    "request_count",
                    "type=integer,time_series_metric=counter",
                    "extra-field-2",
                    "type=keyword"
                )
                .get();
        }

        Map<String, String> hostToClusters = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            hostToClusters.put("p" + i, randomFrom("qa", "prod"));
        }
        long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        int numDocs = between(20, 100);
        docs.clear();
        Map<String, Integer> requestCounts = new HashMap<>();
        List<String> allProjects = List.of("project-1", "project-2", "project-3");
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
                ByteSizeValue memory = ByteSizeValue.ofBytes(randomIntBetween(1024, 1024 * 1024));
                List<String> projects = randomSubsetOf(between(1, 3), allProjects);
                docs.add(new Doc(projects, host, hostToClusters.get(host), timestamp, requestCount, cpu, memory));
            }
        }
        for (String index : List.of("index-1", "index-2")) {
            Randomness.shuffle(docs);
            for (Doc doc : docs) {
                client().prepareIndex(index)
                    .setSource(
                        "@timestamp",
                        doc.timestamp,
                        "project",
                        doc.project,
                        "host",
                        doc.host,
                        "cluster",
                        doc.cluster,
                        "cpu",
                        doc.cpu,
                        "memory",
                        doc.memory.getBytes(),
                        "request_count",
                        doc.requestCount
                    )
                    .get();
            }
            client().admin().indices().prepareRefresh(index).get();
        }
    }

    public void testMetricsInfo() {
        for (String index : List.of("index-1", "index-2", "index-1,index-2")) {
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("TS " + index + " | METRICS_INFO");
            if (canUseQueryPragmas()) {
                request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 1).build()));
            }
            try (var resp = run(request)) {
                List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
                assertThat(rows, not(empty()));
            }
        }
    }

    public void testTsInfo() {
        for (String index : List.of("index-1", "index-2", "index-1,index-2")) {
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("TS " + index + " | TS_INFO");
            if (canUseQueryPragmas()) {
                request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 1).build()));
            }
            try (var resp = run(request)) {
                List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
                assertThat(rows, not(empty()));
            }
        }
    }
}
