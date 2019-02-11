/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
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
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class IndicesLifecycleListenerSingleNodeTests extends ESSingleNodeTestCase {

    public void testStartDeleteIndexEventCallback() throws Throwable {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen();
        Index idx = resolveIndex("test");
        IndexMetaData metaData = indicesService.indexService(idx).getMetaData();
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
            public void beforeIndexShardCreated(ShardId shardId, Settings indexSettings) {
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
            IndexService index = indicesService.createIndex(metaData, Arrays.asList(countingListener));
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
            shard.recoverFromStore();
            newRouting = ShardRoutingHelper.moveToStarted(newRouting);
            IndexShardTestCase.updateRoutingEntry(shard, newRouting);
            assertEquals(6, counter.get());
        } finally {
            indicesService.removeIndex(idx, DELETED, "simon says");
        }
        assertEquals(10, counter.get());
    }

}
