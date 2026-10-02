/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A conflicted sub-field ({@code dim.sub}: keyword vs long across two indices) nested under a healthy time-series dimension
 * must not crash a {@code TS} query when the parent is serialized to a remote data node.
 */
public class TsConflictedDimensionSubfieldTransportIT extends AbstractEsqlIntegTestCase {

    public void testTsConflictedDimensionSubfieldSerializesToRemoteDataNode() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String nodeA = randomDataNode().getName();
        String nodeB = randomValueOtherThan(nodeA, () -> randomDataNode().getName());

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("conflict-a")
                .setSettings(
                    Settings.builder()
                        .put("mode", "time_series")
                        .putList("routing_path", List.of("host"))
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", nodeA)
                )
                .setMapping("""
                    {
                      "properties": {
                        "@timestamp": { "type": "date" },
                        "host":       { "type": "keyword", "time_series_dimension": true },
                        "dim":        { "type": "keyword", "time_series_dimension": true,
                                        "fields": { "sub": { "type": "keyword" } } },
                        "metric":     { "type": "long", "time_series_metric": "gauge" },
                        "req":        { "type": "long", "time_series_metric": "counter" }
                      }
                    }""")
        );

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("conflict-b")
                .setSettings(
                    Settings.builder()
                        .put("mode", "time_series")
                        .putList("routing_path", List.of("host"))
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", nodeB)
                )
                .setMapping("""
                    {
                      "properties": {
                        "@timestamp": { "type": "date" },
                        "host":       { "type": "keyword", "time_series_dimension": true },
                        "dim":        { "type": "keyword", "time_series_dimension": true,
                                        "fields": { "sub": { "type": "long" } } },
                        "metric":     { "type": "long", "time_series_metric": "gauge" },
                        "req":        { "type": "long", "time_series_metric": "counter" }
                      }
                    }""")
        );

        // time_series mode derives _id from the tsid + @timestamp, so it must not be set explicitly.
        long now = System.currentTimeMillis();
        // dim is copied into the dim.sub multi-field, which is long on conflict-b, so its value must parse as a long.
        client().prepareIndex("conflict-a").setSource("@timestamp", now, "host", "h1", "dim", "1", "metric", 10, "req", 100).get();
        client().prepareIndex("conflict-b").setSource("@timestamp", now, "host", "h2", "dim", "2", "metric", 20, "req", 200).get();
        refresh("conflict-a", "conflict-b");

        // Shapes that keep dim in the plan (WHERE/BY/KEEP/raw) exercise the crash; shapes that prune it are the contrast.
        List<String> tsQueries = List.of(
            "TS conflict-a,conflict-b | WHERE dim == \"1\" | STATS s = sum(metric) BY bucket(@timestamp, 1 minute) | LIMIT 10",
            "TS conflict-a,conflict-b | STATS s = sum(rate(req)) BY bucket(@timestamp, 1 minute) | LIMIT 10",
            "TS conflict-a,conflict-b | STATS s = sum(metric) BY bucket(@timestamp, 1 minute) | LIMIT 10",
            "TS conflict-a,conflict-b | STATS s = sum(metric) BY host | LIMIT 10",
            "TS conflict-a,conflict-b | STATS s = sum(metric) BY dim | LIMIT 10",
            "TS conflict-a,conflict-b | KEEP dim, metric | LIMIT 10",
            "TS conflict-a,conflict-b | LIMIT 10"
        );
        StringBuilder all = new StringBuilder();
        boolean crashed = false;
        for (String q : tsQueries) {
            String outcome = describeOutcome(q);
            all.append(outcome).append("----\n");
            crashed |= tripsGuard(outcome);
        }
        // No TS shape may trip the coordinator-only serialization guard.
        assertFalse("A TS query tripped the coordinator-only serialization guard. Outcomes:\n" + all, crashed);

        // FROM does not force-resolve dim, so it never trips the guard.
        assertFalse(tripsGuard(describeOutcome("FROM conflict-a,conflict-b | STATS s = sum(metric) BY host | LIMIT 10")));

        // A kept-parent shape completes and returns rows.
        try (
            var resp = run(
                "TS conflict-a,conflict-b | WHERE dim == \"1\" | STATS s = sum(metric) BY bucket(@timestamp, 1 minute) | LIMIT 10"
            )
        ) {
            assertFalse("kept-parent TS query should not be partial", resp.isPartial());
            assertThat(getValuesList(resp).size(), greaterThan(0));
        }
    }

    /** Flattens a query's outcome (exception cause-chain, or partiality + execution-info) so one string can be asserted. */
    private String describeOutcome(String query) {
        StringBuilder sb = new StringBuilder("query: ").append(query).append('\n');
        try (var resp = run(query)) {
            sb.append("returned: isPartial=").append(resp.isPartial()).append('\n');
            sb.append("executionInfo=").append(resp.getExecutionInfo()).append('\n');
        } catch (Exception e) {
            for (Throwable t = e; t != null; t = t.getCause()) {
                sb.append("caused by: ").append(t.getClass().getName()).append(": ").append(t.getMessage()).append('\n');
            }
        }
        return sb.toString();
    }

    /** A top-level dimension type conflict (keyword vs long) must not crash a {@code TS} query. */
    public void testTsConflictedTopLevelDimensionSerializesToRemoteDataNode() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String nodeA = randomDataNode().getName();
        String nodeB = randomValueOtherThan(nodeA, () -> randomDataNode().getName());

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("tconflict-a")
                .setSettings(
                    Settings.builder()
                        .put("mode", "time_series")
                        .putList("routing_path", List.of("host"))
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", nodeA)
                )
                .setMapping("""
                    {
                      "properties": {
                        "@timestamp": { "type": "date" },
                        "host":       { "type": "keyword", "time_series_dimension": true },
                        "role":       { "type": "keyword", "time_series_dimension": true },
                        "metric":     { "type": "long", "time_series_metric": "gauge" }
                      }
                    }""")
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("tconflict-b")
                .setSettings(
                    Settings.builder()
                        .put("mode", "time_series")
                        .putList("routing_path", List.of("host"))
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", nodeB)
                )
                .setMapping("""
                    {
                      "properties": {
                        "@timestamp": { "type": "date" },
                        "host":       { "type": "keyword", "time_series_dimension": true },
                        "role":       { "type": "long", "time_series_dimension": true },
                        "metric":     { "type": "long", "time_series_metric": "gauge" }
                      }
                    }""")
        );

        long now = System.currentTimeMillis();
        client().prepareIndex("tconflict-a").setSource("@timestamp", now, "host", "h1", "role", "web", "metric", 10).get();
        client().prepareIndex("tconflict-b").setSource("@timestamp", now, "host", "h2", "role", 7, "metric", 20).get();
        refresh("tconflict-a", "tconflict-b");

        List<String> tsQueries = List.of(
            "TS tconflict-a,tconflict-b | STATS s = sum(metric) BY bucket(@timestamp, 1 minute) | LIMIT 10",
            "TS tconflict-a,tconflict-b | STATS s = sum(metric) BY host | LIMIT 10",
            "TS tconflict-a,tconflict-b | LIMIT 10"
        );
        StringBuilder all = new StringBuilder();
        boolean crashed = false;
        for (String q : tsQueries) {
            String outcome = describeOutcome(q);
            all.append(outcome).append("----\n");
            crashed |= tripsGuard(outcome);
        }
        assertFalse("A top-level-dimension TS shape tripped the coordinator guard. Outcomes:\n" + all, crashed);
    }

    private static boolean tripsGuard(String outcome) {
        return outcome.contains("must never leave the coordinator") || outcome.contains("shouldn't be transported");
    }

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }
}
