/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodePrevalidateShardPathResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateShardPathRequest;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateShardPathResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.TransportPrevalidateShardPathAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/*
 * We rely on the shard directory being deleted after the relocation. This removal sometimes fails
 * with "java.io.IOException: access denied" when using WindowsFS which seems to be a known issue.
 * See {@link FileSystemUtilsTests}.
 */
@LuceneTestCase.SuppressFileSystems(value = "WindowsFS")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PrevalidateShardPathIT extends ESIntegTestCase {

    public void testCheckShards() throws Exception {
        internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        String indexName = "index1";
        int index1shards = randomIntBetween(1, 5);
        createIndex("index1", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, index1shards).build());
        ensureGreen(indexName);
        var shardIds = clusterService().state()
            .routingTable()
            .allShards(indexName)
            .stream()
            .map(ShardRouting::shardId)
            .collect(Collectors.toSet());
        String node1Id = getNodeId(node1);
        String node2Id = getNodeId(node2);
        Set<ShardId> shardIdsToCheck = new HashSet<>(shardIds);
        boolean includeUnknownShardId = randomBoolean();
        if (includeUnknownShardId) {
            shardIdsToCheck.add(new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(), randomIntBetween(0, 10)));
        }
        PrevalidateShardPathRequest req = new PrevalidateShardPathRequest(shardIdsToCheck, node1Id, node2Id);
        PrevalidateShardPathResponse resp = client().execute(TransportPrevalidateShardPathAction.TYPE, req).get();
        var nodeResponses = resp.getNodes();
        assertThat(nodeResponses.size(), equalTo(2));
        assertThat(nodeResponses.stream().map(r -> r.getNode().getId()).collect(Collectors.toSet()), equalTo(Set.of(node1Id, node2Id)));
        assertTrue(resp.failures().isEmpty());
        for (NodePrevalidateShardPathResponse nodeResponse : nodeResponses) {
            assertThat(nodeResponse.getShardIds(), equalTo(shardIds));
        }
        // Check that after relocation the source node doesn't have the shard path
        internalCluster().startDataOnlyNode();
        ensureStableCluster(4);
        logger.info("--> relocating shards from the node {}", node2);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node2), indexName);
        ensureGreen(indexName);
        logger.info("--> green after relocation");
        assertBusy(() -> {
            try {
                // The excluded node should eventually delete the shards
                assertNoShards(shardIdsToCheck, node2Id);
            } catch (AssertionError e) {
                // Removal of shards which are no longer allocated to the node is attempted on every cluster state change in IndicesStore.
                // If for whatever reason the removal is not successful (e.g. not enough nodes reported that the shards are active) or it
                // temporarily failed to clean up the shard folder, we need to trigger another cluster state change for this removal to
                // finally succeed.
                logger.info("--> Triggering an extra cluster state update: {}", e.getMessage());
                updateIndexSettings(
                    Settings.builder().put("index.routing.allocation.exclude.name", "non-existent" + randomAlphaOfLength(5)),
                    indexName
                );
                throw e;
            }
        }, 30, TimeUnit.SECONDS);
    }

    private void assertNoShards(Set<ShardId> shards, String nodeId) throws Exception {
        assertBusy(() -> {
            PrevalidateShardPathRequest req = new PrevalidateShardPathRequest(shards, nodeId);
            PrevalidateShardPathResponse resp = client().execute(TransportPrevalidateShardPathAction.TYPE, req).get();
            assertThat(resp.getNodes().size(), equalTo(1));
            assertThat(resp.getNodes().get(0).getNode().getId(), equalTo(nodeId));
            assertTrue("There should be no failures in the response", resp.failures().isEmpty());
            Set<ShardId> node2ShardIds = resp.getNodes().get(0).getShardIds();
            assertThat(
                Strings.format(
                    "Relocation source node [%s] should have no shards after the relocation, but still got %s",
                    nodeId,
                    node2ShardIds
                ),
                node2ShardIds,
                is(empty())
            );
        });
    }
}
