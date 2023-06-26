/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
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
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.shutdown.DeleteShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, minNumDataNodes = 2)
public class SearchableSnapshotShutdownIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), ShutdownPlugin.class);
    }

    @Override
    protected int numberOfShards() {
        // use 1 shard per index and instead use multiple indices to have multiple shards.
        return 1;
    }

    public void testAllocationDisabledDuringShutdown() throws Exception {
        final List<String> restoredIndexNames = setupMountedIndices();
        final String[] restoredIndexNamesArray = restoredIndexNames.toArray(new String[0]);
        final Set<String> indexNodes = restoredIndexNames.stream()
            .flatMap(index -> internalCluster().nodesInclude(index).stream())
            .collect(Collectors.toSet());
        final ClusterState state = client().admin().cluster().prepareState().clear().setRoutingTable(true).setNodes(true).get().getState();
        final Map<String, String> nodeNameToId = StreamSupport.stream(state.getNodes().spliterator(), false)
            .collect(Collectors.toMap(DiscoveryNode::getName, DiscoveryNode::getId));

        for (String indexNode : indexNodes) {
            final String indexNodeId = nodeNameToId.get(indexNode);
            putShutdown(indexNodeId);
            final int shards = (int) StreamSupport.stream(state.routingTable().allShards(restoredIndexNamesArray).spliterator(), false)
                .filter(s -> indexNodeId.equals(s.currentNodeId()))
                .count();
            assert shards > 0;

            assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
            assertExecutorIsIdle(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME);
            waitForBlobCacheFillsToComplete();
            final CacheService cacheService = internalCluster().getInstance(CacheService.class, indexNode);
            cacheService.synchronizeCache();

            logger.info("--> Restarting [{}/{}]", indexNodeId, indexNode);
            internalCluster().restartNode(indexNode, new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    assertBusy(() -> {
                        ClusterHealthResponse response = client().admin().cluster().prepareHealth(restoredIndexNamesArray).get();
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

        ensureGreen(restoredIndexNamesArray);
    }

    private List<String> setupMountedIndices() throws Exception {
        int count = between(1, 10);
        List<String> restoredIndices = new ArrayList<>();
        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(repositoryName, "mock");

        for (int i = 0; i < count; ++i) {
            final String indexName = "index_" + i;
            createAndPopulateIndex(indexName, Settings.builder());

            final SnapshotId snapshotId = createSnapshot(repositoryName, "snapshot-" + i, org.elasticsearch.core.List.of(indexName))
                .snapshotId();
            assertAcked(client().admin().indices().prepareDelete(indexName));
            restoredIndices.add(mountSnapshot(repositoryName, snapshotId.getName(), indexName, Settings.EMPTY));
        }
        return restoredIndices;
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
