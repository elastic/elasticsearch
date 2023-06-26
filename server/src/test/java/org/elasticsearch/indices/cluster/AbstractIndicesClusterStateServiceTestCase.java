/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.GlobalCheckpointSyncer;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndex;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.Shard;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Abstract base class for tests against {@link IndicesClusterStateService}
 */
public abstract class AbstractIndicesClusterStateServiceTestCase extends ESTestCase {

    private boolean enableRandomFailures;

    @Before
    public void injectRandomFailures() {
        enableRandomFailures = randomBoolean();
    }

    protected void disableRandomFailures() {
        enableRandomFailures = false;
    }

    protected void failRandomly() {
        if (enableRandomFailures && rarely()) {
            throw new RuntimeException("dummy test failure");
        }
    }

    /**
     * Checks if cluster state matches internal state of IndicesClusterStateService instance
     *
     * @param state cluster state used for matching
     */
    public void assertClusterStateMatchesNodeState(ClusterState state, IndicesClusterStateService indicesClusterStateService) {
        MockIndicesService indicesService = (MockIndicesService) indicesClusterStateService.indicesService;
        ConcurrentMap<ShardId, ShardRouting> failedShardsCache = indicesClusterStateService.failedShardsCache;
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.getNodes().getLocalNodeId());
        if (localRoutingNode != null) {
            if (enableRandomFailures == false) {
                // initializing a shard should succeed when enableRandomFailures is disabled
                // active shards can be failed if state persistence was disabled in an earlier CS update
                if (failedShardsCache.values().stream().anyMatch(ShardRouting::initializing)) {
                    fail("failed shard cache should not contain initializing shard routing: " + failedShardsCache.values());
                }
            }
            // check that all shards in local routing nodes have been allocated
            for (ShardRouting shardRouting : localRoutingNode) {
                Index index = shardRouting.index();
                IndexMetadata indexMetadata = state.metadata().getIndexSafe(index);

                MockIndexShard shard = indicesService.getShardOrNull(shardRouting.shardId());
                ShardRouting failedShard = failedShardsCache.get(shardRouting.shardId());

                if (state.blocks().disableStatePersistence()) {
                    if (shard != null) {
                        fail("Shard with id " + shardRouting + " should be removed from indicesService due to disabled state persistence");
                    }
                } else {
                    if (failedShard != null && failedShard.isSameAllocation(shardRouting) == false) {
                        fail("Shard cache has not been properly cleaned for " + failedShard);
                    }
                    if (shard == null && failedShard == null) {
                        // shard must either be there or there must be a failure
                        fail("Shard with id " + shardRouting + " expected but missing in indicesService and failedShardsCache");
                    }
                    if (enableRandomFailures == false) {
                        if (shard == null && shardRouting.initializing() && failedShard == shardRouting) {
                            // initializing a shard should succeed when enableRandomFailures is disabled
                            fail("Shard with id " + shardRouting + " expected but missing in indicesService " + failedShard);
                        }
                    }

                    if (shard != null) {
                        AllocatedIndex<? extends Shard> indexService = indicesService.indexService(index);
                        assertTrue("Index " + index + " expected but missing in indicesService", indexService != null);

                        // index metadata has been updated
                        assertThat(indexService.getIndexSettings().getIndexMetadata(), equalTo(indexMetadata));
                        // shard has been created
                        if (enableRandomFailures == false || failedShard == null) {
                            assertTrue("Shard with id " + shardRouting + " expected but missing in indexService", shard != null);
                            // shard has latest shard routing
                            assertThat(shard.routingEntry(), equalTo(shardRouting));
                        }

                        if (shard.routingEntry().primary() && shard.routingEntry().active()) {
                            IndexShardRoutingTable shardRoutingTable = state.routingTable().shardRoutingTable(shard.shardId());
                            Set<String> inSyncIds = state.metadata()
                                .index(shard.shardId().getIndex())
                                .inSyncAllocationIds(shard.shardId().id());
                            assertThat(
                                shard.routingEntry() + " isn't updated with in-sync aIDs",
                                shard.inSyncAllocationIds,
                                equalTo(inSyncIds)
                            );
                            assertThat(
                                shard.routingEntry() + " isn't updated with routing table",
                                shard.routingTable,
                                equalTo(shardRoutingTable)
                            );
                        }
                    }
                }
            }
        }

        // all other shards / indices have been cleaned up
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            final Index index = indexService.getIndexSettings().getIndex();
            if (state.blocks().disableStatePersistence()) {
                fail("Index service " + index + " should be removed from indicesService due to disabled state persistence");
            }

            assertTrue(state.metadata().getIndexSafe(index) != null);

            boolean shardsFound = false;
            for (Shard shard : indexService) {
                shardsFound = true;
                ShardRouting persistedShardRouting = shard.routingEntry();
                ShardRouting shardRouting = localRoutingNode.getByShardId(persistedShardRouting.shardId());
                if (shardRouting == null) {
                    fail("Shard with id " + persistedShardRouting + " locally exists but missing in routing table");
                }
                if (shardRouting.equals(persistedShardRouting) == false) {
                    fail("Local shard " + persistedShardRouting + " has stale routing" + shardRouting);
                }
            }

            if (shardsFound == false) {
                // check if we have shards of that index in failedShardsCache
                // if yes, we might not have cleaned the index as failedShardsCache can be populated by another thread
                assertFalse(failedShardsCache.keySet().stream().noneMatch(shardId -> shardId.getIndex().equals(index)));
            }

        }
    }

    /**
     * Mock for {@link IndicesService}
     */
    protected class MockIndicesService implements AllocatedIndices<MockIndexShard, MockIndexService> {
        private volatile Map<String, MockIndexService> indices = emptyMap();

        @Override
        public synchronized MockIndexService createIndex(
            IndexMetadata indexMetadata,
            List<IndexEventListener> buildInIndexListener,
            boolean writeDanglingIndices
        ) throws IOException {
            MockIndexService indexService = new MockIndexService(new IndexSettings(indexMetadata, Settings.EMPTY));
            indices = Maps.copyMapWithAddedEntry(indices, indexMetadata.getIndexUUID(), indexService);
            return indexService;
        }

        @Override
        public IndexMetadata verifyIndexIsDeleted(Index index, ClusterState state) {
            return null;
        }

        @Override
        public void deleteUnassignedIndex(String reason, IndexMetadata metadata, ClusterState clusterState) {

        }

        @Override
        public synchronized void removeIndex(Index index, IndexRemovalReason reason, String extraInfo) {
            if (hasIndex(index)) {
                Map<String, MockIndexService> newIndices = new HashMap<>(indices);
                newIndices.remove(index.getUUID());
                indices = unmodifiableMap(newIndices);
            }
        }

        @Override
        @Nullable
        public MockIndexService indexService(Index index) {
            return indices.get(index.getUUID());
        }

        @Override
        public MockIndexShard createShard(
            final ShardRouting shardRouting,
            final PeerRecoveryTargetService recoveryTargetService,
            final PeerRecoveryTargetService.RecoveryListener recoveryListener,
            final RepositoriesService repositoriesService,
            final Consumer<IndexShard.ShardFailure> onShardFailure,
            final GlobalCheckpointSyncer globalCheckpointSyncer,
            final RetentionLeaseSyncer retentionLeaseSyncer,
            final DiscoveryNode targetNode,
            final DiscoveryNode sourceNode
        ) throws IOException {
            failRandomly();
            RecoveryState recoveryState = new RecoveryState(shardRouting, targetNode, sourceNode);
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
        public void updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) throws IOException {
            failRandomly();
        }

        @Override
        public void updateMetadata(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) {
            indexSettings.updateIndexMetadata(newIndexMetadata);
            for (MockIndexShard shard : shards.values()) {
                shard.updateTerm(newIndexMetadata.primaryTerm(shard.shardId().id()));
            }
        }

        @Override
        public MockIndexShard getShardOrNull(int shardId) {
            return shards.get(shardId);
        }

        public synchronized MockIndexShard createShard(ShardRouting routing) throws IOException {
            failRandomly();
            MockIndexShard shard = new MockIndexShard(routing, indexSettings.getIndexMetadata().primaryTerm(routing.shardId().id()));
            shards = Maps.copyMapWithAddedEntry(shards, routing.id(), shard);
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
    }

    /**
     * Mock for {@link IndexShard}
     */
    protected class MockIndexShard implements IndicesClusterStateService.Shard {
        private volatile ShardRouting shardRouting;
        private volatile RecoveryState recoveryState;
        private volatile Set<String> inSyncAllocationIds;
        private volatile IndexShardRoutingTable routingTable;
        private volatile long term;

        public MockIndexShard(ShardRouting shardRouting, long term) {
            this.shardRouting = shardRouting;
            this.term = term;
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
        public void updateShardState(
            ShardRouting shardRouting,
            long newPrimaryTerm,
            BiConsumer<IndexShard, ActionListener<ResyncTask>> primaryReplicaSyncer,
            long applyingClusterStateVersion,
            Set<String> inSyncAllocationIds,
            IndexShardRoutingTable routingTable
        ) throws IOException {
            failRandomly();
            assertThat(this.shardId(), equalTo(shardRouting.shardId()));
            assertTrue("current: " + this.shardRouting + ", got: " + shardRouting, this.shardRouting.isSameAllocation(shardRouting));
            if (this.shardRouting.active()) {
                assertTrue(
                    "an active shard must stay active, current: " + this.shardRouting + ", got: " + shardRouting,
                    shardRouting.active()
                );
            }
            if (this.shardRouting.primary()) {
                assertTrue("a primary shard can't be demoted", shardRouting.primary());
                if (this.shardRouting.initializing()) {
                    assertEquals("primary term can not be updated on an initializing primary shard: " + shardRouting, term, newPrimaryTerm);
                }
            } else if (shardRouting.primary()) {
                // note: it's ok for a replica in post recovery to be started and promoted at once
                // this can happen when the primary failed after we sent the start shard message
                assertTrue(
                    "a replica can only be promoted when active. current: " + this.shardRouting + " new: " + shardRouting,
                    shardRouting.active()
                );
            }
            this.shardRouting = shardRouting;
            if (shardRouting.primary()) {
                term = newPrimaryTerm;
                this.inSyncAllocationIds = inSyncAllocationIds;
                this.routingTable = routingTable;
            }
        }

        @Override
        public ShardRouting routingEntry() {
            return shardRouting;
        }

        @Override
        public IndexShardState state() {
            return null;
        }

        public long term() {
            return term;
        }

        public void updateTerm(long newTerm) {
            assertThat("term can only be incremented: " + shardRouting, newTerm, greaterThanOrEqualTo(term));
            if (shardRouting.primary() && shardRouting.active()) {
                assertThat("term can not be changed on an active primary shard: " + shardRouting, newTerm, equalTo(term));
            }
            this.term = newTerm;
        }

        @Override
        public ShardLongFieldRange getTimestampRange() {
            return ShardLongFieldRange.EMPTY;
        }

    }
}
