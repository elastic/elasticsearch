/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.searchablesnapshots.BaseSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.shutdown.DeleteShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, minNumDataNodes = 2)
public class SearchableSnapshotShutdownIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), ShutdownPlugin.class);
    }

    public void testAllocationDisabledDuringShutdown() throws Exception {
        final String restoredIndexName = setupMountedIndex();
        final Set<String> indexNodes = internalCluster().nodesInclude(restoredIndexName);
        final ClusterState state = client().admin().cluster().prepareState().clear().setRoutingTable(true).setNodes(true).get().getState();
        final Map<String, String> nodeNameToId = state.getNodes()
            .stream()
            .collect(Collectors.toMap(DiscoveryNode::getName, DiscoveryNode::getId));

        for (String indexNode : indexNodes) {
            final String indexNodeId = nodeNameToId.get(indexNode);
            putShutdown(indexNodeId);
            final int shards = (int) state.routingTable()
                .allShards(restoredIndexName)
                .stream()
                .filter(s -> indexNodeId.equals(s.currentNodeId()))
                .count();
            assert shards > 0;

            final CacheService cacheService = internalCluster().getInstance(CacheService.class, indexNode);
            cacheService.synchronizeCache();

            logger.info("--> Restarting [{}/{}]", indexNodeId, indexNode);
            internalCluster().restartNode(indexNode, new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    assertBusy(() -> {
                        ClusterHealthResponse response = client().admin()
                            .cluster()
                            .health(Requests.clusterHealthRequest(restoredIndexName))
                            .actionGet();
                        assertThat(response.getUnassignedShards(), Matchers.equalTo(shards));
                    });
                    return super.onNodeStopped(nodeName);
                }
            });
            // leave shutdown in place for some nodes to check that shards get assigned anyway.
            if (randomBoolean()) {
                removeShutdown(indexNodeId);
            }
        }

        ensureGreen(restoredIndexName);
    }

    private String setupMountedIndex() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createAndPopulateIndex(indexName, Settings.builder());

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(repositoryName, "mock");

        final SnapshotId snapshotId = createSnapshot(repositoryName, "snapshot-1", List.of(indexName)).snapshotId();
        assertAcked(client().admin().indices().prepareDelete(indexName));
        return mountSnapshot(repositoryName, snapshotId.getName(), indexName, Settings.EMPTY);
    }

    private void putShutdown(String nodeToRestartId) throws InterruptedException, ExecutionException {
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            nodeToRestartId,
            SingleNodeShutdownMetadata.Type.RESTART,
            this.getTestName(),
            null,
            null
        );
        assertTrue(client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get().isAcknowledged());
    }

    private void removeShutdown(String node) throws ExecutionException, InterruptedException {
        assertTrue(client().execute(DeleteShutdownNodeAction.INSTANCE, new DeleteShutdownNodeAction.Request(node)).get().isAcknowledged());
    }
}
