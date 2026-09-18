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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A keyword multi-field whose sub-field conflicts (text vs keyword) across indices must not crash when its healthy
 * parent serializes to a remote data node.
 */
public class ConflictedMultifieldTransportIT extends AbstractEsqlIntegTestCase {

    public void testConflictedMultifieldSerializesToRemoteDataNode() {
        setupTwoPinnedConflictIndices("""
            {
              "properties": {
                "my_field": { "type": "keyword", "fields": { "analyzed": { "type": "text" } } }
              }
            }""", """
            {
              "properties": {
                "my_field": { "type": "keyword", "fields": { "analyzed": { "type": "keyword" } } }
              }
            }""");

        // SORT keeps my_field in the plan, dragging its conflicted analyzed sub-field onto the wire pre-fix.
        String query = "FROM conflict-a,conflict-b | SORT my_field | LIMIT 2";
        try (var resp = run(query)) {
            assertFalse("query should not be partial: " + resp.getExecutionInfo(), resp.isPartial());
            assertThat(getValuesList(resp).size(), greaterThan(0));
        }
    }

    /**
     * A text field's only keyword sub-field conflicts across indices, so the field loses its "exact sub-field": pushdown of
     * sort/equality/LIKE, which normally rewrites to the exact sub-field, must fall back to the compute engine and still be correct.
     */
    public void testExactSubfieldUnusableWhenConflicted() {
        setupTwoPinnedConflictIndices("""
            {
              "properties": {
                "my_field": { "type": "text", "fields": { "analyzed": { "type": "keyword" } } }
              }
            }""", """
            {
              "properties": {
                "my_field": { "type": "text", "fields": { "analyzed": { "type": "text" } } }
              }
            }""");

        try (var resp = run("FROM conflict-a,conflict-b | SORT my_field | KEEP my_field | LIMIT 2")) {
            assertFalse("query should not be partial: " + resp.getExecutionInfo(), resp.isPartial());
            assertThat(getValuesList(resp), equalTo(List.of(List.of("hello"), List.of("world"))));
        }
        try (var resp = run("FROM conflict-a,conflict-b | WHERE my_field == \"world\" | KEEP my_field | LIMIT 2")) {
            assertFalse("query should not be partial: " + resp.getExecutionInfo(), resp.isPartial());
            assertThat(getValuesList(resp), equalTo(List.of(List.of("world"))));
        }
        try (var resp = run("FROM conflict-a,conflict-b | WHERE my_field LIKE \"he*\" | KEEP my_field | LIMIT 2")) {
            assertFalse("query should not be partial: " + resp.getExecutionInfo(), resp.isPartial());
            assertThat(getValuesList(resp), equalTo(List.of(List.of("hello"))));
        }
    }

    /** Creates conflict-a/conflict-b pinned to two different data nodes (guaranteeing cross-node transport) with one doc each. */
    private void setupTwoPinnedConflictIndices(String mappingA, String mappingB) {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String nodeA = randomDataNode().getName();
        String nodeB = randomValueOtherThan(nodeA, () -> randomDataNode().getName());
        createPinnedIndex("conflict-a", nodeA, mappingA);
        createPinnedIndex("conflict-b", nodeB, mappingB);

        client().prepareIndex("conflict-a").setSource("my_field", "hello").get();
        client().prepareIndex("conflict-b").setSource("my_field", "world").get();
        refresh("conflict-a", "conflict-b");
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
