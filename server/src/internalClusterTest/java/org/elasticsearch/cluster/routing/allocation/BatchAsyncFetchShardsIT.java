/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.TransportNodesBatchListGatewayStartedShards;
import org.elasticsearch.gateway.TransportNodesBatchListGatewayStartedShards.NodeGatewayBatchStartedShard;
import org.elasticsearch.gateway.TransportNodesBatchListGatewayStartedShards.NodesGatewayBatchStartedShards;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.TransportNodesBatchListShardStoreMetadata;
import org.elasticsearch.indices.store.TransportNodesBatchListShardStoreMetadata.NodesBatchStoreFilesMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata.StoreFilesMetadata;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3)
public class BatchAsyncFetchShardsIT extends ESIntegTestCase {

    private Settings buildSettings(boolean batchMode, int batchStepSize) {
        return Settings.builder()
            .put(AllocationService.CLUSTER_ROUTING_ALLOCATION_BATCH_FETCH_SHARD_ENABLE_SETTING.getKey(), batchMode)
            .put(AllocationService.CLUSTER_ROUTING_ALLOCATION_BATCH_FETCH_SHARD_STEP_SIZE_SETTING.getKey(), batchStepSize)
            .build();
    }

    private Settings buildNullSettings() {
        return Settings.builder()
            .putNull(AllocationService.CLUSTER_ROUTING_ALLOCATION_BATCH_FETCH_SHARD_ENABLE_SETTING.getKey())
            .putNull(AllocationService.CLUSTER_ROUTING_ALLOCATION_BATCH_FETCH_SHARD_STEP_SIZE_SETTING.getKey())
            .build();
    }

    @After
    public void clearSettings() {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(buildNullSettings())
            .setPersistentSettings(buildNullSettings())
            .get();
    }

    /**
     * Test random async batch fetch shards.
     */
    public void testRandomBatchMode() throws Exception {
        // random batch
        Settings settings = buildSettings(randomBoolean(), 10000);
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).setPersistentSettings(settings).get();
        createIndex("test", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 10).put(SETTING_NUMBER_OF_REPLICAS, 2).build());

        ensureGreen();
        internalCluster().fullRestart();
        ensureGreen();
    }

    /**
     * Test batch step size.
     * in flush process would assert the queued requests is less or equal to batch step size
     */
    public void testBatchStepSize() throws Exception {
        Settings settings = buildSettings(true, 4);
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).setPersistentSettings(settings).get();
        createIndex("test", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 6).put(SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureGreen();

        client().admin().cluster().prepareState().get().getState();
        internalCluster().fullRestart();
        ensureGreen();

        settings = buildSettings(true, 10);
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).setPersistentSettings(settings).get();
        internalCluster().fullRestart();
        ensureGreen();
    }

    public void testAsyncBatchFetchPrimaries() {
        createIndex("test", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen();

        final Index index = resolveIndex("test");
        Map<ShardId, String> shards = new HashMap<>();
        shards.put(new ShardId(index, 0), "");
        shards.put(new ShardId(index, 1), "");
        shards.put(new ShardId(index, 2), "");

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        DiscoveryNode node = state.getNodes().getDataNodes().values().iterator().next();

        NodesGatewayBatchStartedShards response;
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesBatchListGatewayStartedShards.class),
            new TransportNodesBatchListGatewayStartedShards.Request(shards, new DiscoveryNode[] { node })
        );

        // each node contains 3 shard entry, only got one primary shard
        assertThat(response.getNodes(), hasSize(1));
        assertThat(response.getNodes().get(0).getStartedShards(), hasSize(3));
        int count = 0;
        for (NodeGatewayBatchStartedShard shard : response.getNodes().get(0).getStartedShards()) {
            if (shard.primary()) {
                count++;
            }
        }
        assertEquals(1, count);
    }

    public void testAsyncBatchListShardStore() {
        createIndex("test", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureGreen();

        final Index index = resolveIndex("test");
        Map<ShardId, String> shards = new HashMap<>();
        shards.put(new ShardId(index, 0), "");
        shards.put(new ShardId(index, 1), "");
        shards.put(new ShardId(index, 2), "");

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        DiscoveryNode node = state.getNodes().getDataNodes().values().iterator().next();

        NodesBatchStoreFilesMetadata response;
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesBatchListShardStoreMetadata.class),
            new TransportNodesBatchListShardStoreMetadata.Request(shards, new DiscoveryNode[] { node })
        );

        // each node contains 3 shard entry, got two shards store
        assertThat(response.getNodes(), hasSize(1));
        assertThat(response.getNodes().get(0).storeFilesMetadataList(), hasSize(3));
        int count = 0;
        for (StoreFilesMetadata shardStore : response.getNodes().get(0).storeFilesMetadataList()) {
            if (shardStore.isEmpty() == false) {
                count++;
            }
        }
        assertEquals(2, count);
    }
}
