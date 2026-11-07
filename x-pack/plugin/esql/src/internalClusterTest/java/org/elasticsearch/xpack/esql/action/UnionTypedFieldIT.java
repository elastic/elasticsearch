/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
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

    /**
     * Renaming a union column is refused, and the message is the deliverable: without this check the query fails
     * twice over, first as a dump of internal ids from the optimizer's consistency check (the plan says
     * {@code unsupported} where the field says {@code union}) and then, past that, as the renderer throwing on the
     * data node because an aliased column has no field to carry the leg types. Both were measured before the check
     * existed. This is a stated deviation from "a union is always renderable", not a hidden one - see the check's own
     * javadoc for what compliance would take.
     */
    public void testRenamingAUnionColumnIsRefusedWithBothWaysThrough() {
        setupConflictingIndices();
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> run("FROM union-a,union-b | RENAME duration AS d | KEEP d")
        );
        assertThat(e.getMessage(), containsString("Column [duration] holds more than one type, so it cannot be renamed"));
        assertThat(e.getMessage(), containsString("TYPED(duration, \"<type>\")"));
        assertThat(e.getMessage(), containsString("duration::<type>"));
    }

    /** Both escapes the message offers have to work, or it is sending the user nowhere. */
    public void testTheEscapesOfferedForARenameWork() {
        setupConflictingIndices();
        // TYPED selects the long-mapped values and nulls the rest, so the two keyword rows collapse to one null.
        try (var response = run("FROM union-a,union-b | EVAL d = TYPED(duration, \"long\") | KEEP d")) {
            assertColumnTypes(response.columns(), List.of("long"));
            assertThat(
                Set.copyOf(getValuesList(response)),
                equalTo(Set.of(List.of(200L), List.of(201L), List.of(202L), Collections.singletonList(null)))
            );
        }
        // The cast converts instead of selecting, so "two hundred" is not a long and warns rather than erroring.
        try (var response = run("FROM union-a,union-b | EVAL d = duration::long | KEEP d")) {
            assertColumnTypes(response.columns(), List.of("long"));
            assertThat(
                Set.copyOf(getValuesList(response)),
                equalTo(Set.of(List.of(200L), List.of(201L), List.of(202L), Collections.singletonList(null)))
            );
        }
    }

    /**
     * A union column that is not the join key rides through a {@code LOOKUP JOIN} unchanged. The refusal that exists is
     * on the key only - {@code Join.UNSUPPORTED_TYPES} is consulted against {@code config.rightFields()}, so a payload
     * column is never looked at - and the block side of it is inherited rather than written: a union is a compound
     * block and {@code AbstractDelegatingCompoundBlock.filter} maps over its sub-blocks, which is the call the join's
     * left-hand side makes on every page. This is what makes that a measured fact rather than a reading of the code.
     */
    public void testAUnionColumnThatIsNotTheJoinKeyRidesThroughALookupJoin() {
        setupConflictingIndices();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("union-lookup")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.mode", "lookup"))
                .setMapping("""
                    { "properties": { "tag": { "type": "keyword" }, "note": { "type": "keyword" } } }""")
        );
        // Only one of the two tags is in the lookup index on purpose: every left row matching would let the join keep
        // whole pages, and the path worth testing is the one that rebuilds them - the left-hand blocks are filtered
        // and null-filled per position, which is where a compound block with a tag vector would come apart.
        client().prepareIndex("union-lookup").setSource("tag", "a", "note", "from a").get();
        refresh("union-lookup");

        try (var response = run("FROM union-a,union-b | LOOKUP JOIN union-lookup ON tag | KEEP duration, note")) {
            assertFalse("the query should not be partial: " + response.getExecutionInfo(), response.isPartial());
            assertColumnTypes(response.columns(), List.of("union", "keyword"));
            assertThat(
                "the legs survive the join, so the column is still declared as the field holds it",
                response.columns().getFirst().originalTypes(),
                equalTo(List.of("keyword", "long"))
            );
            // The three long-legged rows join; the two keyword-legged ones miss and keep a null note. Every value
            // survives on its own leg either way. No SORT: a union has no order.
            assertThat(
                Set.copyOf(getValuesList(response).stream().map(row -> List.of(row.get(0), String.valueOf(row.get(1)))).toList()),
                equalTo(
                    Set.of(
                        List.of(200L, "from a"),
                        List.of(201L, "from a"),
                        List.of(202L, "from a"),
                        List.of("two hundred", "null"),
                        List.of("a while", "null")
                    )
                )
            );
        }
    }

    /** Two indices pinned to two different data nodes, disagreeing about one field, one document each. */
    private void setupConflictingIndices() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String nodeA = randomDataNode().getName();
        String nodeB = randomValueOtherThan(nodeA, () -> randomDataNode().getName());
        createPinnedIndex("union-a", nodeA, """
            { "properties": { "duration": { "type": "long" }, "tag": { "type": "keyword" } } }""");
        createPinnedIndex("union-b", nodeB, """
            { "properties": { "duration": { "type": "keyword" }, "tag": { "type": "keyword" } } }""");
        for (int d = 0; d < 3; d++) {
            client().prepareIndex("union-a").setSource("duration", 200 + d, "tag", "a").get();
        }
        for (String value : List.of("two hundred", "a while")) {
            client().prepareIndex("union-b").setSource("duration", value, "tag", "b").get();
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
