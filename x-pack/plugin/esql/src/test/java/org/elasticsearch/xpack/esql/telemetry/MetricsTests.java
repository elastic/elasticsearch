/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.telemetry.KibanaExecutionContextClassifier.Feature;
import org.elasticsearch.xpack.esql.telemetry.KibanaExecutionContextClassifier.Interaction;

import java.util.BitSet;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

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

    public void testEveryKibanaFeatureIsEmittedAtZero() {
        Counters stats = newMetrics().stats();
        assertThat(nested(stats, "kibana_features"), aMapWithSize(Feature.values().length));
        for (Feature feature : Feature.values()) {
            assertThat(stats.get("kibana_features." + feature + ".total"), equalTo(0L));
            assertThat(stats.get("kibana_features." + feature + ".failed"), equalTo(0L));
        }
    }

    public void testEveryKibanaInteractionIsEmittedAtZero() {
        Counters stats = newMetrics().stats();
        assertThat(nested(stats, "kibana_interactions"), aMapWithSize(Interaction.values().length));
        for (Interaction interaction : Interaction.values()) {
            assertThat(stats.get("kibana_interactions." + interaction + ".total"), equalTo(0L));
            assertThat(stats.get("kibana_interactions." + interaction + ".failed"), equalTo(0L));
        }
    }

    public void testKibanaFeatureIncrementsOnlyItsFeatureAndInteraction() {
        Metrics metrics = newMetrics();
        Feature feature = randomFrom(Feature.values());
        metrics.total(feature);
        metrics.total(feature);
        metrics.failed(feature);
        Counters stats = metrics.stats();
        for (Feature other : Feature.values()) {
            assertThat(stats.get("kibana_features." + other + ".total"), equalTo(other == feature ? 2L : 0L));
            assertThat(stats.get("kibana_features." + other + ".failed"), equalTo(other == feature ? 1L : 0L));
        }
        for (Interaction interaction : Interaction.values()) {
            boolean own = interaction == feature.interaction();
            assertThat(stats.get("kibana_interactions." + interaction + ".total"), equalTo(own ? 2L : 0L));
            assertThat(stats.get("kibana_interactions." + interaction + ".failed"), equalTo(own ? 1L : 0L));
        }
    }

    public void testNoQueryPathsBeforeAnyQuery() {
        assertThat(newMetrics().stats().toNestedMap(), not(hasKey("query_paths")));
    }

    public void testQueryPathCountsTotalAndTheCommandsItUsed() {
        Metrics metrics = newMetrics();
        QueryPath path = new QueryPath(QueryMetric.KIBANA, Feature.DISCOVER.toString(), LeadingCommand.FROM);
        metrics.total(path, commands(FeatureMetric.FROM, FeatureMetric.WHERE));
        metrics.total(path, commands(FeatureMetric.FROM, FeatureMetric.STATS));
        assertThat(
            nested(metrics.stats(), "query_paths"),
            equalTo(
                Map.of(
                    "kibana",
                    Map.of("discover", Map.of("from", Map.of("total", 2L, "commands", Map.of("from", 2L, "where", 1L, "stats", 1L))))
                )
            )
        );
    }

    public void testFailedQueryPathCountsFailed() {
        Metrics metrics = newMetrics();
        QueryPath path = new QueryPath(QueryMetric.REST, DirectClient.PYTHON.toString(), LeadingCommand.TS);
        metrics.total(path, commands());
        metrics.failed(path);
        assertThat(
            nested(metrics.stats(), "query_paths"),
            equalTo(Map.of("rest", Map.of("python", Map.of("ts", Map.of("total", 1L, "failed", 1L)))))
        );
    }

    public void testQueryPathsAreCountedApart() {
        Metrics metrics = newMetrics();
        metrics.total(new QueryPath(QueryMetric.FLEET, QueryPath.NO_SOURCE, LeadingCommand.ROW), commands(FeatureMetric.ROW));
        metrics.total(new QueryPath(QueryMetric.FLEET, QueryPath.NO_SOURCE, LeadingCommand.FROM), commands());
        metrics.total(new QueryPath(QueryMetric.OTHER, QueryPath.NO_SOURCE, LeadingCommand.ROW), commands());
        assertThat(
            nested(metrics.stats(), "query_paths"),
            equalTo(
                Map.of(
                    "fleet",
                    Map.of("_none", Map.of("row", Map.of("total", 1L, "commands", Map.of("row", 1L)), "from", Map.of("total", 1L))),
                    "other",
                    Map.of("_none", Map.of("row", Map.of("total", 1L)))
                )
            )
        );
    }

    public void testConcurrentQueryPathCountsAreExact() throws Exception {
        Metrics metrics = newMetrics();
        QueryPath path = new QueryPath(QueryMetric.REST, DirectClient.CURL.toString(), LeadingCommand.FROM);
        int threads = between(2, 8);
        int perThread = between(100, 1000);
        startInParallel(threads, t -> {
            for (int i = 0; i < perThread; i++) {
                metrics.total(path, commands(FeatureMetric.WHERE));
                metrics.failed(path);
            }
        });
        Counters stats = metrics.stats();
        long expected = (long) threads * perThread;
        assertThat(stats.get("query_paths.rest.curl.from.total"), equalTo(expected));
        assertThat(stats.get("query_paths.rest.curl.from.failed"), equalTo(expected));
        assertThat(stats.get("query_paths.rest.curl.from.commands.where"), equalTo(expected));
    }

    private static BitSet commands(FeatureMetric... used) {
        BitSet bits = new BitSet(FeatureMetric.values().length);
        for (FeatureMetric metric : used) {
            bits.set(metric.ordinal());
        }
        return bits;
    }

    private static Metrics newMetrics() {
        return new Metrics(TEST_FUNCTION_REGISTRY, randomBoolean(), randomBoolean());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> nested(Counters stats, String key) {
        return (Map<String, Object>) stats.toNestedMap().get(key);
    }
}
