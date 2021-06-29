/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class IndicesLifecycleListenerSingleNodeTests extends ESSingleNodeTestCase {

    public void testStartDeleteIndexEventCallback() throws Throwable {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen();
        Index idx = resolveIndex("test");
        IndexMetadata metadata = indicesService.indexService(idx).getMetadata();
        ShardRouting shardRouting = indicesService.indexService(idx).getShard(0).routingEntry();
        final AtomicInteger counter = new AtomicInteger(1);
        IndexEventListener countingListener = new IndexEventListener() {

            @Override
            public void beforeIndexCreated(Index index, Settings indexSettings) {
                assertEquals("test", index.getName());
                assertEquals(1, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void afterIndexCreated(IndexService indexService) {
                assertEquals("test", indexService.index().getName());
                assertEquals(2, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void beforeIndexShardCreated(ShardRouting shardRouting, Settings indexSettings) {
                assertEquals(3, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void afterIndexShardCreated(IndexShard indexShard) {
                assertEquals(4, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void afterIndexShardStarted(IndexShard indexShard) {
                assertEquals(5, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
                assertEquals(DELETED, reason);
                assertEquals(6, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {
                assertEquals(7, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
                assertEquals(8, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void afterIndexRemoved(Index index, IndexSettings indexSettings, IndexRemovalReason reason) {
                assertEquals(DELETED, reason);
                assertEquals(9, counter.get());
                counter.incrementAndGet();
            }

        };
        indicesService.removeIndex(idx, DELETED, "simon says");
        try {
            IndexService index = indicesService.createIndex(metadata, Arrays.asList(countingListener), false);
            assertEquals(3, counter.get());
            idx = index.index();
            ShardRouting newRouting = shardRouting;
            String nodeId = newRouting.currentNodeId();
            UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "boom");
            newRouting = newRouting.moveToUnassigned(unassignedInfo)
                .updateUnassigned(unassignedInfo, RecoverySource.EmptyStoreRecoverySource.INSTANCE);
            newRouting = ShardRoutingHelper.initialize(newRouting, nodeId);
            IndexShard shard = index.createShard(newRouting, s -> {}, RetentionLeaseSyncer.EMPTY);
            IndexShardTestCase.updateRoutingEntry(shard, newRouting);
            assertEquals(5, counter.get());
            final DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
                    emptyMap(), emptySet(), Version.CURRENT);
            shard.markAsRecovering("store", new RecoveryState(newRouting, localNode, null));
            IndexShardTestCase.recoverFromStore(shard);
            newRouting = ShardRoutingHelper.moveToStarted(newRouting);
            IndexShardTestCase.updateRoutingEntry(shard, newRouting);
            assertEquals(6, counter.get());
        } finally {
            indicesService.removeIndex(idx, DELETED, "simon says");
        }
        assertEquals(10, counter.get());
    }

}
