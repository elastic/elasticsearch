/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanationUtils;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodePrevalidateShardPathResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateShardPathRequest;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateShardPathResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.TransportPrevalidateShardPathAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

/*
 * We rely on the shard directory being deleted after the relocation. This removal sometimes fails
 * with "java.io.IOException: access denied" when using WindowsFS which seems to be a known issue.
 * See {@link FileSystemUtilsTests}.
 */
@LuceneTestCase.SuppressFileSystems(value = "WindowsFS")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PrevalidateShardPathIT extends ESIntegTestCase {

    @TestIssueLogging(
        value = "org.elasticsearch.cluster.service.MasterService:DEBUG, "
            + "org.elasticsearch.indices.store.IndicesStore:TRACE,"
            + "org.elasticsearch.indices.cluster.IndicesClusterStateService:DEBUG,"
            + "org.elasticsearch.indices.IndicesService:TRACE",
        issueUrl = "https://github.com/elastic/elasticsearch/issues/104807"
    )
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
        String node3 = internalCluster().startDataOnlyNode();
        ensureStableCluster(4);
        logger.info("Relocating shards from the node {}", node2);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node2), indexName);
        ensureGreen(indexName);
        assertBusy(() -> {
            try {
                // The excluded node should eventually delete the shards
                PrevalidateShardPathRequest req2 = new PrevalidateShardPathRequest(shardIdsToCheck, node2Id);
                PrevalidateShardPathResponse resp2 = client().execute(TransportPrevalidateShardPathAction.TYPE, req2).get();
                assertThat(resp2.getNodes().size(), equalTo(1));
                assertThat(resp2.getNodes().get(0).getNode().getId(), equalTo(node2Id));
                assertTrue("There should be no failures in the response", resp.failures().isEmpty());
                Set<ShardId> node2ShardIds = resp2.getNodes().get(0).getShardIds();
                if (node2ShardIds.size() > 0) {
                    logger.info(
                        "Relocation source node {} should have no shards after the relocation, but still got {}",
                        node2Id,
                        node2ShardIds
                    );
                    List<ShardRouting> node2Shards = clusterService().state()
                        .routingTable()
                        .allShards()
                        .filter(s -> s.getIndexName().equals(indexName))
                        .filter(s -> node2ShardIds.contains(s.shardId()))
                        .toList();
                    logger.info("Found {} shards on the relocation source node {} in the cluster state", node2Shards, node2Id);
                    for (var node2Shard : node2Shards) {
                        var explanation = ClusterAllocationExplanationUtils.getClusterAllocationExplanation(
                            client(),
                            node2Shard.getIndexName(),
                            node2Shard.id(),
                            node2Shard.primary()
                        );
                        logger.info(
                            "Shard: {} is still located on relocation source node: {}. Allocation explanation: {}",
                            node2Shard.shardId(),
                            node2,
                            Strings.toString(ChunkedToXContent.wrapAsToXContent(explanation), false, true)
                        );
                    }
                    throw new AssertionError("The relocation source node should have removed the shard(s)");
                }
            } catch (AssertionError e) {
                // Removal of shards which are no longer allocated to the node is attempted on every cluster state change in IndicesStore.
                // If for whatever reason the removal is not triggered (e.g. not enough nodes reported that the shards are active) or it
                // temporarily failed to clean up the shard folder, we need to trigger another cluster state change for this removal to
                // finally succeed.
                logger.info("Triggering an extra cluster state update");
                updateIndexSettings(
                    Settings.builder().put("index.routing.allocation.exclude.name", "non-existent" + randomAlphaOfLength(5)),
                    indexName
                );
                throw e;
            }
        }, 30, TimeUnit.SECONDS);
    }
}
