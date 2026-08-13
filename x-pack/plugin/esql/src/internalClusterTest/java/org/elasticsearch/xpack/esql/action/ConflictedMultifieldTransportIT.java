/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A keyword multi-field whose sub-field conflicts (text vs keyword) across indices must not crash when its healthy
 * parent serializes to a remote data node.
 */
public class ConflictedMultifieldTransportIT extends AbstractEsqlIntegTestCase {

    public void testConflictedMultifieldSerializesToRemoteDataNode() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String nodeA = randomDataNode().getName();
        String nodeB = randomValueOtherThan(nodeA, () -> randomDataNode().getName());

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("conflict-a")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", nodeA)
                )
                .setMapping("""
                    {
                      "properties": {
                        "my_field": { "type": "keyword", "fields": { "analyzed": { "type": "text" } } }
                      }
                    }""")
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("conflict-b")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.require._name", nodeB)
                )
                .setMapping("""
                    {
                      "properties": {
                        "my_field": { "type": "keyword", "fields": { "analyzed": { "type": "keyword" } } }
                      }
                    }""")
        );

        client().prepareIndex("conflict-a").setSource("my_field", "hello").get();
        client().prepareIndex("conflict-b").setSource("my_field", "world").get();
        refresh("conflict-a", "conflict-b");

        // SORT keeps my_field in the plan, dragging its conflicted analyzed sub-field onto the wire pre-fix.
        String query = "FROM conflict-a,conflict-b | SORT my_field | LIMIT 2";
        try (var resp = run(query)) {
            assertFalse("query should not be partial: " + resp.getExecutionInfo(), resp.isPartial());
            assertThat(getValuesList(resp).size(), greaterThan(0));
        }
    }

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }
}
