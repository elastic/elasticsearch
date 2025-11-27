/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;

public class ExtractDimensionFieldsAfterAggregationIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        Settings settings = Settings.builder().put("mode", "time_series").putList("routing_path", List.of("host", "cluster")).build();

        client().admin()
            .indices()
            .prepareCreate("metrics")
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
                "memory",
                "type=long,time_series_metric=gauge",
                "request_count",
                "type=integer,time_series_metric=counter"
            )
            .get();

        long baseTimestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-01-01T00:00:00Z");

        String[] hosts = { "host-1", "host-2", "host-3" };
        String[] clusters = { "prod", "qa" };
        int timeBuckets = 5;

        for (String host : hosts) {
            for (String cluster : clusters) {
                for (int bucket = 0; bucket < timeBuckets; bucket++) {
                    long timestamp = baseTimestamp + (bucket * 3600_000L);

                    client().prepareIndex("metrics")
                        .setSource(
                            "@timestamp",
                            timestamp,
                            "host",
                            host,
                            "cluster",
                            cluster,
                            "cpu",
                            50.0 + bucket,
                            "memory",
                            1024L * (bucket + 1),
                            "request_count",
                            bucket * 100
                        )
                        .get();
                }
            }
        }

        client().admin().indices().prepareRefresh("metrics").get();
    }

    public void test() {
        String query = "TS metrics | STATS sum_over_time(cpu) BY tbucket(1h)";

        try (EsqlQueryResponse resp = run(query)) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            for (List<Object> row : rows) {
                System.out.println("  " + row);
            }
        }
    }
}
