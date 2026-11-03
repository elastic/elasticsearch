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

    public void testQueriesHasEveryClientAndAll() {
        Map<String, Object> queries = nested(newMetrics().stats(), "queries");
        assertThat(queries, aMapWithSize(QueryMetric.values().length + 1));
        for (QueryMetric client : QueryMetric.values()) {
            assertThat(queries, hasKey(client.toString()));
        }
        assertThat(queries, hasKey("_all"));
    }

    public void testEveryEntryPointIsEmittedAtZero() {
        Counters stats = newMetrics().stats();
        assertThat(nested(stats, "entry_points"), aMapWithSize(QueryEntryPoint.values().length));
        for (QueryEntryPoint entryPoint : QueryEntryPoint.values()) {
            assertThat(stats.get("entry_points." + entryPoint + ".total"), equalTo(0L));
            assertThat(stats.get("entry_points." + entryPoint + ".failed"), equalTo(0L));
        }
    }

    public void testEveryLeadingCommandIsEmittedAtZero() {
        Counters stats = newMetrics().stats();
        assertThat(nested(stats, "leading_commands"), aMapWithSize(LeadingCommand.values().length));
        for (LeadingCommand command : LeadingCommand.values()) {
            assertThat(stats.get("leading_commands." + command + ".total"), equalTo(0L));
            assertThat(stats.get("leading_commands." + command + ".failed"), equalTo(0L));
        }
    }

    public void testAllIsTheSumOfClients() {
        Metrics metrics = newMetrics();
        metrics.total(QueryMetric.KIBANA);
        metrics.total(QueryMetric.KIBANA);
        metrics.failed(QueryMetric.KIBANA);
        metrics.total(QueryMetric.OTHER);
        metrics.total(QueryMetric.REST);
        metrics.failed(QueryMetric.REST);
        Counters stats = metrics.stats();
        assertThat(stats.get("queries.kibana.total"), equalTo(2L));
        assertThat(stats.get("queries.kibana.failed"), equalTo(1L));
        assertThat(stats.get("queries.other.total"), equalTo(1L));
        assertThat(stats.get("queries.other.failed"), equalTo(0L));
        assertThat(stats.get("queries.rest.total"), equalTo(1L));
        assertThat(stats.get("queries.rest.failed"), equalTo(1L));
        assertThat(stats.get("queries._all.total"), equalTo(4L));
        assertThat(stats.get("queries._all.failed"), equalTo(2L));
    }

    public void testEntryPointIncrementsOnlyItsOwnCounter() {
        Metrics metrics = newMetrics();
        QueryEntryPoint entryPoint = randomFrom(QueryEntryPoint.values());
        metrics.total(entryPoint);
        metrics.total(entryPoint);
        metrics.failed(entryPoint);
        Counters stats = metrics.stats();
        for (QueryEntryPoint other : QueryEntryPoint.values()) {
            long expectedTotal = other == entryPoint ? 2L : 0L;
            long expectedFailed = other == entryPoint ? 1L : 0L;
            assertThat(stats.get("entry_points." + other + ".total"), equalTo(expectedTotal));
            assertThat(stats.get("entry_points." + other + ".failed"), equalTo(expectedFailed));
        }
    }

    public void testEveryDirectClientIsEmittedAtZero() {
        Counters stats = newMetrics().stats();
        assertThat(nested(stats, "direct_clients"), aMapWithSize(DirectClient.values().length));
        for (DirectClient directClient : DirectClient.values()) {
            assertThat(stats.get("direct_clients." + directClient + ".total"), equalTo(0L));
            assertThat(stats.get("direct_clients." + directClient + ".failed"), equalTo(0L));
        }
    }

    public void testDirectClientIncrementsOnlyItsOwnCounter() {
        Metrics metrics = newMetrics();
        DirectClient directClient = randomFrom(DirectClient.values());
        metrics.total(directClient);
        metrics.total(directClient);
        metrics.failed(directClient);
        Counters stats = metrics.stats();
        for (DirectClient other : DirectClient.values()) {
            long expectedTotal = other == directClient ? 2L : 0L;
            long expectedFailed = other == directClient ? 1L : 0L;
            assertThat(stats.get("direct_clients." + other + ".total"), equalTo(expectedTotal));
            assertThat(stats.get("direct_clients." + other + ".failed"), equalTo(expectedFailed));
        }
    }

    public void testLeadingCommandIncrementsOnlyItsOwnCounter() {
        Metrics metrics = newMetrics();
        LeadingCommand command = randomFrom(LeadingCommand.values());
        metrics.total(command);
        metrics.total(command);
        metrics.failed(command);
        Counters stats = metrics.stats();
        for (LeadingCommand other : LeadingCommand.values()) {
            long expectedTotal = other == command ? 2L : 0L;
            long expectedFailed = other == command ? 1L : 0L;
            assertThat(stats.get("leading_commands." + other + ".total"), equalTo(expectedTotal));
            assertThat(stats.get("leading_commands." + other + ".failed"), equalTo(expectedFailed));
        }
    }

    private static Metrics newMetrics() {
        return new Metrics(TEST_FUNCTION_REGISTRY, randomBoolean(), randomBoolean());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> nested(Counters stats, String key) {
        return (Map<String, Object>) stats.toNestedMap().get(key);
    }
}
