/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

public class MetricsTests extends ESTestCase {

    public void testQueriesHasEveryClientRestAndAll() {
        Map<String, Object> queries = nested(newMetrics().stats(), "queries");
        assertThat(queries, aMapWithSize(QueryMetric.values().length + 2));
        for (QueryMetric client : QueryMetric.values()) {
            assertThat(queries, hasKey(client.toString()));
        }
        assertThat(queries, hasKey("rest"));
        assertThat(queries, hasKey("_all"));
    }

    public void testAllIsTheSumOfClients() {
        Metrics metrics = newMetrics();
        metrics.total(QueryMetric.KIBANA);
        metrics.total(QueryMetric.KIBANA);
        metrics.failed(QueryMetric.KIBANA);
        metrics.total(QueryMetric.OTHER);
        metrics.total(QueryMetric.REST_ORIGIN);
        metrics.failed(QueryMetric.REST_ORIGIN);
        Counters stats = metrics.stats();
        assertThat(stats.get("queries.kibana.total"), equalTo(2L));
        assertThat(stats.get("queries.kibana.failed"), equalTo(1L));
        assertThat(stats.get("queries.other.total"), equalTo(1L));
        assertThat(stats.get("queries.other.failed"), equalTo(0L));
        assertThat(stats.get("queries.rest_origin.total"), equalTo(1L));
        assertThat(stats.get("queries.rest_origin.failed"), equalTo(1L));
        assertThat(stats.get("queries._all.total"), equalTo(4L));
        assertThat(stats.get("queries._all.failed"), equalTo(2L));
    }

    public void testRestCountsEveryQuery() {
        Metrics metrics = newMetrics();
        int queries = between(1, 20);
        int failed = 0;
        for (int i = 0; i < queries; i++) {
            QueryMetric client = randomFrom(QueryMetric.values());
            metrics.total(client);
            if (randomBoolean()) {
                metrics.failed(client);
                failed++;
            }
        }
        Counters stats = metrics.stats();
        assertThat(stats.get("queries.rest.total"), equalTo((long) queries));
        assertThat(stats.get("queries.rest.failed"), equalTo((long) failed));
        assertThat(stats.get("queries.rest.total"), equalTo(stats.get("queries._all.total")));
        assertThat(stats.get("queries.rest.failed"), equalTo(stats.get("queries._all.failed")));
    }

    private static Metrics newMetrics() {
        return new Metrics(TEST_FUNCTION_REGISTRY, randomBoolean(), randomBoolean());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> nested(Counters stats, String key) {
        return (Map<String, Object>) stats.toNestedMap().get(key);
    }
}
