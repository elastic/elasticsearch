/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.junit.annotations.TestLogging;

import static org.hamcrest.Matchers.equalTo;

/**
 * Renames an index in a stateless cluster (one index node, one search node) and checks that writes, refreshes and searches keep
 * working through the index and search tiers afterwards.
 */
@TestLogging(
    value = "org.elasticsearch.action.bulk:DEBUG,org.elasticsearch.indices.cluster:DEBUG,org.elasticsearch.cluster.service:DEBUG,"
        + "org.elasticsearch.cluster.coordination:DEBUG,org.elasticsearch.xpack.stateless:DEBUG",
    reason = "diagnose hangs after renaming an index in a stateless cluster"
)
public class StatelessRenameIndexIT extends AbstractStatelessPluginIntegTestCase {

    /**
     * The rename is acknowledged once every node applied it, and the search copies keep their routing entries, so a search under
     * the new name must work immediately, without waiting for green.
     */
    public void testSearchImmediatelyAfterRename() {
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        final String original = "immediate-original";
        final String renamed = "immediate-renamed";
        createIndex(original, indexSettings(2, 1).build(), false);
        ensureGreen(original);
        client().prepareIndex(original)
            .setSource("field", "before")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get(TimeValue.timeValueSeconds(30));
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(original), 1L);

        assertTrue(
            client().execute(TransportIndicesAliasesAction.TYPE, renameIndexRequest(original, renamed))
                .actionGet(TimeValue.timeValueSeconds(30))
                .isAcknowledged()
        );
        final var routingTable = clusterService().state().routingTable().index(renamed);
        assertNotNull(routingTable);
        for (int id = 0; id < routingTable.size(); id++) {
            final IndexShardRoutingTable shardTable = routingTable.shard(id);
            for (int copy = 0; copy < shardTable.size(); copy++) {
                final ShardRouting shard = shardTable.shard(copy);
                assertTrue(shard + " should be started", shard.state() == ShardRoutingState.STARTED);
            }
        }
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(renamed), 1L);
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(renamed).setSize(0), 1L);
    }

    public void testWriteAndSearchAfterRename() {
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        final String original = "stateless-rename-original";
        final String renamed = "stateless-rename-renamed";
        // opt out of the test framework rename so that the rename below is the only one
        createIndex(original, indexSettings(1, 1).build(), false);
        ensureGreen(original);
        client().prepareIndex(original)
            .setSource("field", "before")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get(TimeValue.timeValueSeconds(30));
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(original), 1L);

        var response = client().execute(TransportIndicesAliasesAction.TYPE, renameIndexRequest(original, renamed))
            .actionGet(TimeValue.timeValueSeconds(30));
        assertTrue(response.isAcknowledged());
        ensureGreen(renamed);

        // a write with a dynamic mapping update, then a write without one
        client().prepareIndex(renamed).setSource("newfield", "after").get(TimeValue.timeValueSeconds(30));
        client().prepareIndex(renamed).setSource("field", "after").get(TimeValue.timeValueSeconds(30));
        assertThat(indicesAdmin().prepareRefresh(renamed).get(TimeValue.timeValueSeconds(30)).getFailedShards(), equalTo(0));
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(renamed), 3L);
    }
}
