/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * A field two indices disagree about, read as a union rather than erased. The two indices are pinned to two different
 * data nodes, so every value crosses the wire from a node that had to decide its leg from its own mapping alone - which
 * is the only arrangement that tests the thing the type is for, and the one a single-node or single-shard test cannot.
 */
public class UnionTypedFieldIT extends AbstractEsqlIntegTestCase {

    @Override
    protected QueryPragmas getPragmas() {
        // The type is behind this pragma and the pragma is honoured only in a snapshot build, which is also the only
        // build the type itself is supported on, so the two gates coincide.
        return new QueryPragmas(Settings.builder().put(randomPragmas().getSettings()).put("union_typed_fields", true).build());
    }

    public void testEachIndexsValuesRenderAsItsOwnType() {
        setupConflictingIndices();
        try (var response = run("FROM union-a,union-b | KEEP duration")) {
            assertFalse("the query should not be partial: " + response.getExecutionInfo(), response.isPartial());
            assertColumnNames(response.columns(), List.of("duration"));
            assertColumnTypes(response.columns(), List.of("union"));
            assertThat(
                "the legs are declared in the order a tag indexes",
                response.columns().getFirst().originalTypes(),
                equalTo(List.of("keyword", "long"))
            );
            // No SORT, because a union has no order; the rows come back in whatever order the two nodes answer in.
            assertThat(
                Set.copyOf(getValuesList(response)),
                equalTo(Set.of(List.of(200L), List.of(201L), List.of(202L), List.of("two hundred"), List.of("a while")))
            );
        }
    }

    /**
     * The per-value type, which is the only way to tell two values of one column apart without converting either. It is
     * also what makes the column groupable without grouping a union at all: {@code TYPEOF} is a keyword, so the
     * aggregation never sees a union block.
     */
    public void testTypeOfNamesTheTypeEachIndexGaveTheValue() {
        setupConflictingIndices();
        try (var response = run("FROM union-a,union-b | EVAL t = TYPEOF(duration) | KEEP duration, t")) {
            assertFalse("the query should not be partial: " + response.getExecutionInfo(), response.isPartial());
            assertColumnTypes(response.columns(), List.of("union", "keyword"));
            assertThat(
                Set.copyOf(getValuesList(response)),
                equalTo(
                    Set.of(
                        List.of(200L, "long"),
                        List.of(201L, "long"),
                        List.of(202L, "long"),
                        List.of("two hundred", "keyword"),
                        List.of("a while", "keyword")
                    )
                )
            );
        }
    }

    /** Counting by type: what ClickHouse spells {@code count() ... GROUP BY variantType(v)}. */
    public void testCountByType() {
        setupConflictingIndices();
        try (var response = run("FROM union-a,union-b | STATS c = COUNT(*) BY t = TYPEOF(duration) | SORT t")) {
            assertFalse("the query should not be partial: " + response.getExecutionInfo(), response.isPartial());
            assertColumnNames(response.columns(), List.of("c", "t"));
            assertThat(getValuesList(response), equalTo(List.of(List.of(2L, "keyword"), List.of(3L, "long"))));
        }
    }

    /** Two indices pinned to two different data nodes, disagreeing about one field, one document each. */
    private void setupConflictingIndices() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String nodeA = randomDataNode().getName();
        String nodeB = randomValueOtherThan(nodeA, () -> randomDataNode().getName());
        createPinnedIndex("union-a", nodeA, """
            { "properties": { "duration": { "type": "long" } } }""");
        createPinnedIndex("union-b", nodeB, """
            { "properties": { "duration": { "type": "keyword" } } }""");
        for (int d = 0; d < 3; d++) {
            client().prepareIndex("union-a").setSource("duration", 200 + d).get();
        }
        for (String value : List.of("two hundred", "a while")) {
            client().prepareIndex("union-b").setSource("duration", value).get();
        }
        refresh("union-a", "union-b");
    }

    private void createPinnedIndex(String index, String node, String mapping) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", node)
                )
                .setMapping(mapping)
        );
    }

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }
}
