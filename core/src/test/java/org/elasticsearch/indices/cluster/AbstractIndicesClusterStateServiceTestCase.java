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

package org.elasticsearch.indices.cluster;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndex;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.Shard;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTargetService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 * Abstract base class for tests against {@link IndicesClusterStateService}
 */
public abstract class AbstractIndicesClusterStateServiceTestCase extends ESTestCase {


    protected void failRandomly() {
        if (rarely()) {
            throw new RuntimeException("dummy test failure");
        }
    }

    /**
     * Checks if cluster state matches internal state of IndicesClusterStateService instance
     *
     * @param state cluster state used for matching
     */
    public static void assertClusterStateMatchesNodeState(ClusterState state, IndicesClusterStateService indicesClusterStateService) {
        AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>> indicesService =
            indicesClusterStateService.indicesService;
        ConcurrentMap<ShardId, ShardRouting> failedShardsCache = indicesClusterStateService.failedShardsCache;
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.getNodes().getLocalNodeId());
        if (localRoutingNode != null) {
            // check that all shards in local routing nodes have been allocated
            for (ShardRouting shardRouting : localRoutingNode) {
                Index index = shardRouting.index();
                IndexMetaData indexMetaData = state.metaData().getIndexSafe(index);

                Shard shard = indicesService.getShardOrNull(shardRouting.shardId());
                ShardRouting failedShard = failedShardsCache.get(shardRouting.shardId());
                if (shard == null && failedShard == null) {
                    fail("Shard with id " + shardRouting + " expected but missing in indicesService and failedShardsCache");
                }
                if (failedShard != null && failedShard.isSameAllocation(shardRouting) == false) {
                    fail("Shard cache has not been properly cleaned for " + failedShard);
                }

                if (shard != null) {
                    AllocatedIndex<? extends Shard> indexService = indicesService.indexService(index);
                    assertTrue("Index " + index + " expected but missing in indicesService", indexService != null);

                    // index metadata has been updated
                    assertThat(indexService.getIndexSettings().getIndexMetaData(), equalTo(indexMetaData));
                    // shard has been created
                    if (failedShard == null) {
                        assertTrue("Shard with id " + shardRouting + " expected but missing in indexService",
                            shard != null);
                        // shard has latest shard routing
                        assertThat(shard.routingEntry(), equalTo(shardRouting));
                    }
                }
            }
        }

        // all other shards / indices have been cleaned up
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            assertTrue(state.metaData().getIndexSafe(indexService.index()) != null);

            boolean shardsFound = false;
            for (Shard shard : indexService) {
                shardsFound = true;
                ShardRouting persistedShardRouting = shard.routingEntry();
                boolean found = false;
                for (ShardRouting shardRouting : localRoutingNode) {
                    if (persistedShardRouting.equals(shardRouting)) {
                        found = true;
                    }
                }
                assertTrue(found);
            }

            if (shardsFound == false) {
                // check if we have shards of that index in failedShardsCache
                // if yes, we might not have cleaned the index as failedShardsCache can be populated by another thread
                assertFalse(failedShardsCache.keySet().stream().noneMatch(shardId -> shardId.getIndex().equals(indexService.index())));
            }

        }
    }

    /**
     * Mock for {@link IndicesService}
     */
    protected class MockIndicesService implements AllocatedIndices<MockIndexShard, MockIndexService> {
        private volatile Map<String, MockIndexService> indices = emptyMap();

        @Override
        public synchronized MockIndexService createIndex(NodeServicesProvider nodeServicesProvider, IndexMetaData indexMetaData,
                                                         List<IndexEventListener> buildInIndexListener) throws IOException {
            MockIndexService indexService = new MockIndexService(new IndexSettings(indexMetaData, Settings.EMPTY));
            indices = newMapBuilder(indices).put(indexMetaData.getIndexUUID(), indexService).immutableMap();
            return indexService;
        }

        @Override
        public IndexMetaData verifyIndexIsDeleted(Index index, ClusterState state) {
            return null;
        }

        @Override
        public void deleteUnassignedIndex(String reason, IndexMetaData metaData, ClusterState clusterState) {

        }

        @Override
        public synchronized void deleteIndex(Index index, String reason) {
            if (hasIndex(index) == false) {
                return;
            }
            Map<String, MockIndexService> newIndices = new HashMap<>(indices);
            newIndices.remove(index.getUUID());
            indices = unmodifiableMap(newIndices);
        }

        @Override
        public synchronized void removeIndex(Index index, String reason) {
            if (hasIndex(index) == false) {
                return;
            }
            Map<String, MockIndexService> newIndices = new HashMap<>(indices);
            newIndices.remove(index.getUUID());
            indices = unmodifiableMap(newIndices);
        }

        @Override
        public @Nullable MockIndexService indexService(Index index) {
            return indices.get(index.getUUID());
        }

        @Override
        public MockIndexShard createShard(ShardRouting shardRouting, RecoveryState recoveryState,
                                          RecoveryTargetService recoveryTargetService,
                                          RecoveryTargetService.RecoveryListener recoveryListener, RepositoriesService repositoriesService,
                                          NodeServicesProvider nodeServicesProvider, Callback<IndexShard.ShardFailure> onShardFailure)
            throws IOException {
            failRandomly();
            MockIndexService indexService = indexService(recoveryState.getShardId().getIndex());
            MockIndexShard indexShard = indexService.createShard(shardRouting);
            indexShard.recoveryState = recoveryState;
            return indexShard;
        }

        @Override
        public void processPendingDeletes(Index index, IndexSettings indexSettings, TimeValue timeValue) throws IOException,
            InterruptedException {

        }

        private boolean hasIndex(Index index) {
            return indices.containsKey(index.getUUID());
        }

        @Override
        public Iterator<MockIndexService> iterator() {
            return indices.values().iterator();
        }
    }

    /**
     * Mock for {@link IndexService}
     */
    protected class MockIndexService implements AllocatedIndex<MockIndexShard> {
        private volatile Map<Integer, MockIndexShard> shards = emptyMap();

        private final IndexSettings indexSettings;

        public MockIndexService(IndexSettings indexSettings) {
            this.indexSettings = indexSettings;
        }

        @Override
        public IndexSettings getIndexSettings() {
            return indexSettings;
        }

        @Override
        public boolean updateMapping(IndexMetaData indexMetaData) throws IOException {
            failRandomly();
            return false;
        }

        @Override
        public void updateMetaData(IndexMetaData indexMetaData) {
            indexSettings.updateIndexMetaData(indexMetaData);
        }

        @Override
        public MockIndexShard getShardOrNull(int shardId) {
            return shards.get(shardId);
        }

        public synchronized MockIndexShard createShard(ShardRouting routing) throws IOException {
            failRandomly();
            MockIndexShard shard = new MockIndexShard(routing);
            shards = newMapBuilder(shards).put(routing.id(), shard).immutableMap();
            return shard;
        }

        @Override
        public synchronized void removeShard(int shardId, String reason) {
            if (shards.containsKey(shardId) == false) {
                return;
            }
            HashMap<Integer, MockIndexShard> newShards = new HashMap<>(shards);
            MockIndexShard indexShard = newShards.remove(shardId);
            assert indexShard != null;
            shards = unmodifiableMap(newShards);
        }

        @Override
        public Iterator<MockIndexShard> iterator() {
            return shards.values().iterator();
        }

        @Override
        public Index index() {
            return indexSettings.getIndex();
        }
    }

    /**
     * Mock for {@link IndexShard}
     */
    protected class MockIndexShard implements IndicesClusterStateService.Shard {
        private volatile ShardRouting shardRouting;
        private volatile RecoveryState recoveryState;

        public MockIndexShard(ShardRouting shardRouting) {
            this.shardRouting = shardRouting;
        }

        @Override
        public ShardId shardId() {
            return shardRouting.shardId();
        }

        @Override
        public RecoveryState recoveryState() {
            return recoveryState;
        }

        @Override
        public ShardRouting routingEntry() {
            return shardRouting;
        }

        @Override
        public IndexShardState state() {
            return null;
        }

        @Override
        public void updateRoutingEntry(ShardRouting shardRouting) throws IOException {
            failRandomly();
            assert this.shardId().equals(shardRouting.shardId());
            assert this.shardRouting.isSameAllocation(shardRouting);
            this.shardRouting = shardRouting;
        }
    }
}
