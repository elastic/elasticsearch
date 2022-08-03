/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RecoverySource.EmptyStoreRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.LocalShardsRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The {@link IndexRoutingTable} represents routing information for a single
 * index. The routing table maintains a list of all shards in the index. A
 * single shard in this context has one more instances namely exactly one
 * {@link ShardRouting#primary() primary} and 1 or more replicas. In other
 * words, each instance of a shard is considered a replica while only one
 * replica per shard is a {@code primary} replica. The {@code primary} replica
 * can be seen as the "leader" of the shard acting as the primary entry point
 * for operations on a specific shard.
 * <p>
 * Note: The term replica is not directly
 * reflected in the routing table or in related classes, replicas are
 * represented as {@link ShardRouting}.
 * </p>
 */
public class IndexRoutingTable implements SimpleDiffable<IndexRoutingTable> {

    private static final List<Predicate<ShardRouting>> PRIORITY_REMOVE_CLAUSES = List.of(
        ShardRouting::unassigned,
        ShardRouting::initializing,
        shardRouting -> true
    );
    private final Index index;
    private final ShardShuffler shuffler;

    // note, we assume that when the index routing is created, ShardRoutings are created for all possible number of
    // shards with state set to UNASSIGNED
    private final IndexShardRoutingTable[] shards;
    private final boolean allShardsActive;
    private final List<ShardRouting> allActiveShards;

    IndexRoutingTable(Index index, IndexShardRoutingTable[] shards) {
        this.index = index;
        this.shuffler = new RotationShardShuffler(Randomness.get().nextInt());
        this.shards = shards;
        int totalShardCount = 0;
        List<ShardRouting> allActiveShards = new ArrayList<>();
        for (IndexShardRoutingTable shard : shards) {
            allActiveShards.addAll(shard.activeShards());
            totalShardCount += shard.size();
        }
        this.allActiveShards = CollectionUtils.wrapUnmodifiableOrEmptySingleton(allActiveShards);
        this.allShardsActive = totalShardCount == allActiveShards.size();
    }

    /**
     * Return the index id
     *
     * @return id of the index
     */
    public Index getIndex() {
        return index;
    }

    boolean validate(Metadata metadata) {
        // check index exists
        if (metadata.hasIndex(index.getName()) == false) {
            throw new IllegalStateException(index + " exists in routing does not exists in metadata");
        }
        IndexMetadata indexMetadata = metadata.index(index.getName());
        if (indexMetadata.getIndexUUID().equals(index.getUUID()) == false) {
            throw new IllegalStateException(index.getName() + " exists in routing does not exists in metadata with the same uuid");
        }

        // check the number of shards
        if (indexMetadata.getNumberOfShards() != shards.length) {
            Set<Integer> expected = new HashSet<>();
            for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                expected.add(i);
            }
            for (IndexShardRoutingTable indexShardRoutingTable : this.shards) {
                expected.remove(indexShardRoutingTable.shardId().id());
            }
            throw new IllegalStateException("Wrong number of shards in routing table, missing: " + expected);
        }

        // check the replicas
        for (IndexShardRoutingTable indexShardRoutingTable : this.shards) {
            int routingNumberOfReplicas = indexShardRoutingTable.size() - 1;
            if (routingNumberOfReplicas != indexMetadata.getNumberOfReplicas()) {
                throw new IllegalStateException(
                    "Shard ["
                        + indexShardRoutingTable.shardId().id()
                        + "] routing table has wrong number of replicas, expected ["
                        + indexMetadata.getNumberOfReplicas()
                        + "], got ["
                        + routingNumberOfReplicas
                        + "]"
                );
            }
            for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                ShardRouting shardRouting = indexShardRoutingTable.shard(copy);
                if (shardRouting.index().equals(index) == false) {
                    throw new IllegalStateException(
                        "shard routing has an index [" + shardRouting.index() + "] that is different from the routing table"
                    );
                }
                final Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(shardRouting.id());
                if (shardRouting.active() && inSyncAllocationIds.contains(shardRouting.allocationId().getId()) == false) {
                    throw new IllegalStateException(
                        "active shard routing "
                            + shardRouting
                            + " has no corresponding entry in the in-sync allocation set "
                            + inSyncAllocationIds
                    );
                }

                if (shardRouting.primary()
                    && shardRouting.initializing()
                    && shardRouting.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE) {
                    if (inSyncAllocationIds.contains(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID)) {
                        if (inSyncAllocationIds.size() != 1) {
                            throw new IllegalStateException(
                                "a primary shard routing "
                                    + shardRouting
                                    + " is a primary that is recovering from a stale primary has unexpected allocation ids in in-sync "
                                    + "allocation set "
                                    + inSyncAllocationIds
                            );
                        }
                    } else if (inSyncAllocationIds.contains(shardRouting.allocationId().getId()) == false) {
                        throw new IllegalStateException(
                            "a primary shard routing "
                                + shardRouting
                                + " is a primary that is recovering from a known allocation id but has no corresponding "
                                + "entry in the in-sync allocation set "
                                + inSyncAllocationIds
                        );
                    }
                }
            }
        }
        return true;
    }

    /**
     * Calculates the number of nodes that hold one or more shards of this index
     * {@link IndexRoutingTable} excluding the nodes with the node ids give as
     * the <code>excludedNodes</code> parameter.
     *
     * @param excludedNodes id of nodes that will be excluded
     * @return number of distinct nodes this index has at least one shard allocated on
     */
    public int numberOfNodesShardsAreAllocatedOn(String... excludedNodes) {
        Set<String> nodes = new HashSet<>();
        for (IndexShardRoutingTable shardRoutingTable : this.shards) {
            for (int copy = 0; copy < shardRoutingTable.size(); copy++) {
                ShardRouting shardRouting = shardRoutingTable.shard(copy);
                if (shardRouting.assignedToNode()) {
                    String currentNodeId = shardRouting.currentNodeId();
                    boolean excluded = false;
                    if (excludedNodes != null) {
                        for (String excludedNode : excludedNodes) {
                            if (currentNodeId.equals(excludedNode)) {
                                excluded = true;
                                break;
                            }
                        }
                    }
                    if (excluded == false) {
                        nodes.add(currentNodeId);
                    }
                }
            }
        }
        return nodes.size();
    }

    public int size() {
        return shards.length;
    }

    @Nullable
    public IndexShardRoutingTable shard(int shardId) {
        if (shardId > shards.length - 1) {
            return null;
        }
        return shards[shardId];
    }

    /**
     * Returns <code>true</code> if all shards are primary and active. Otherwise <code>false</code>.
     */
    public boolean allPrimaryShardsActive() {
        return primaryShardsActive() == shards.length;
    }

    public boolean allShardsActive() {
        return this.allShardsActive;
    }

    /**
     * Calculates the number of primary shards in active state in routing table
     *
     * @return number of active primary shards
     */
    public int primaryShardsActive() {
        int counter = 0;
        for (IndexShardRoutingTable shardRoutingTable : this.shards) {
            if (shardRoutingTable.primaryShard().active()) {
                counter++;
            }
        }
        return counter;
    }

    /**
     * Returns <code>true</code> if all primary shards are in
     * {@link ShardRoutingState#UNASSIGNED} state. Otherwise <code>false</code>.
     */
    public boolean allPrimaryShardsUnassigned() {
        return primaryShardsUnassigned() == shards.length;
    }

    /**
     * Calculates the number of primary shards in the routing tables that are in
     * {@link ShardRoutingState#UNASSIGNED} state.
     */
    public int primaryShardsUnassigned() {
        int counter = 0;
        for (IndexShardRoutingTable shardRoutingTable : this.shards) {
            if (shardRoutingTable.primaryShard().unassigned()) {
                counter++;
            }
        }
        return counter;
    }

    /**
     * Returns a {@link List} of shards that match one of the states listed in {@link ShardRoutingState states}
     *
     * @param state {@link ShardRoutingState} to retrieve
     * @return a {@link List} of shards that match one of the given {@link ShardRoutingState states}
     */
    public List<ShardRouting> shardsWithState(ShardRoutingState state) {
        List<ShardRouting> shards = new ArrayList<>();
        for (IndexShardRoutingTable shardRoutingTable : this.shards) {
            shards.addAll(shardRoutingTable.shardsWithState(state));
        }
        return shards;
    }

    /**
     * Returns an unordered iterator over all active shards (including replicas).
     */
    public ShardsIterator randomAllActiveShardsIt() {
        return new PlainShardsIterator(shuffler.shuffle(allActiveShards));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexRoutingTable that = (IndexRoutingTable) o;

        if (index.equals(that.index) == false) return false;
        return Arrays.equals(shards, that.shards) != false;
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + Arrays.hashCode(shards);
        return result;
    }

    public static IndexRoutingTable readFrom(StreamInput in) throws IOException {
        Index index = new Index(in);
        Builder builder = new Builder(index);

        int size = in.readVInt();
        builder.ensureShardArray(size);
        for (int i = 0; i < size; i++) {
            builder.addIndexShard(IndexShardRoutingTable.Builder.readFromThin(in, index));
        }

        return builder.build();
    }

    public static Diff<IndexRoutingTable> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(IndexRoutingTable::readFrom, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        index.writeTo(out);
        out.writeArray((o, s) -> IndexShardRoutingTable.Builder.writeToThin(s, o), shards);
    }

    public static Builder builder(Index index) {
        return new Builder(index);
    }

    public static class Builder {

        private final Index index;
        private IndexShardRoutingTable.Builder[] shards;

        public Builder(Index index) {
            this.index = index;
        }

        /**
         * Initializes a new empty index, as if it was created from an API.
         */
        public Builder initializeAsNew(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        }

        /**
         * Initializes an existing index.
         */
        public Builder initializeAsRecovery(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null));
        }

        /**
         * Initializes a new index caused by dangling index imported.
         */
        public Builder initializeAsFromDangling(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.DANGLING_INDEX_IMPORTED, null));
        }

        /**
         * Initializes a new empty index, as a result of opening a closed index.
         */
        public Builder initializeAsFromCloseToOpen(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, null));
        }

        /**
         * Initializes a new empty index, as a result of closing an opened index.
         */
        public Builder initializeAsFromOpenToClose(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CLOSED, null));
        }

        /**
         * Initializes a new empty index, to be restored from a snapshot
         */
        public Builder initializeAsNewRestore(
            IndexMetadata indexMetadata,
            SnapshotRecoverySource recoverySource,
            Set<Integer> ignoreShards
        ) {
            final UnassignedInfo unassignedInfo = new UnassignedInfo(
                UnassignedInfo.Reason.NEW_INDEX_RESTORED,
                "restore_source["
                    + recoverySource.snapshot().getRepository()
                    + "/"
                    + recoverySource.snapshot().getSnapshotId().getName()
                    + "]"
            );
            return initializeAsRestore(indexMetadata, recoverySource, ignoreShards, true, unassignedInfo);
        }

        /**
         * Initializes an existing index, to be restored from a snapshot
         */
        public Builder initializeAsRestore(IndexMetadata indexMetadata, SnapshotRecoverySource recoverySource) {
            final UnassignedInfo unassignedInfo = new UnassignedInfo(
                UnassignedInfo.Reason.EXISTING_INDEX_RESTORED,
                "restore_source["
                    + recoverySource.snapshot().getRepository()
                    + "/"
                    + recoverySource.snapshot().getSnapshotId().getName()
                    + "]"
            );
            return initializeAsRestore(indexMetadata, recoverySource, null, false, unassignedInfo);
        }

        /**
         * Initializes an index, to be restored from snapshot
         */
        private Builder initializeAsRestore(
            IndexMetadata indexMetadata,
            SnapshotRecoverySource recoverySource,
            Set<Integer> ignoreShards,
            boolean asNew,
            UnassignedInfo unassignedInfo
        ) {
            assert indexMetadata.getIndex().equals(index);
            if (shards != null) {
                throw new IllegalStateException("trying to initialize an index with fresh shards, but already has shards created");
            }
            shards = new IndexShardRoutingTable.Builder[indexMetadata.getNumberOfShards()];
            for (int shardNumber = 0; shardNumber < indexMetadata.getNumberOfShards(); shardNumber++) {
                ShardId shardId = new ShardId(index, shardNumber);
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = IndexShardRoutingTable.builder(shardId);
                for (int i = 0; i <= indexMetadata.getNumberOfReplicas(); i++) {
                    boolean primary = i == 0;
                    if (asNew && ignoreShards.contains(shardNumber)) {
                        // This shards wasn't completely snapshotted - restore it as new shard
                        indexShardRoutingBuilder.addShard(
                            ShardRouting.newUnassigned(
                                shardId,
                                primary,
                                primary ? EmptyStoreRecoverySource.INSTANCE : PeerRecoverySource.INSTANCE,
                                unassignedInfo
                            )
                        );
                    } else {
                        indexShardRoutingBuilder.addShard(
                            ShardRouting.newUnassigned(
                                shardId,
                                primary,
                                primary ? recoverySource : PeerRecoverySource.INSTANCE,
                                unassignedInfo
                            )
                        );
                    }
                }
                shards[shardNumber] = indexShardRoutingBuilder;
            }
            return this;
        }

        /**
         * Initializes a new empty index, with an option to control if its from an API or not.
         */
        private Builder initializeEmpty(IndexMetadata indexMetadata, UnassignedInfo unassignedInfo) {
            assert indexMetadata.getIndex().equals(index);
            if (shards != null) {
                throw new IllegalStateException("trying to initialize an index with fresh shards, but already has shards created");
            }
            shards = new IndexShardRoutingTable.Builder[indexMetadata.getNumberOfShards()];
            for (int shardNumber = 0; shardNumber < indexMetadata.getNumberOfShards(); shardNumber++) {
                ShardId shardId = new ShardId(index, shardNumber);
                final RecoverySource primaryRecoverySource;
                if (indexMetadata.inSyncAllocationIds(shardNumber).isEmpty() == false) {
                    // we have previous valid copies for this shard. use them for recovery
                    primaryRecoverySource = ExistingStoreRecoverySource.INSTANCE;
                } else if (indexMetadata.getResizeSourceIndex() != null) {
                    // this is a new index but the initial shards should merged from another index
                    primaryRecoverySource = LocalShardsRecoverySource.INSTANCE;
                } else {
                    // a freshly created index with no restriction
                    primaryRecoverySource = EmptyStoreRecoverySource.INSTANCE;
                }
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = IndexShardRoutingTable.builder(shardId);
                for (int i = 0; i <= indexMetadata.getNumberOfReplicas(); i++) {
                    boolean primary = i == 0;
                    indexShardRoutingBuilder.addShard(
                        ShardRouting.newUnassigned(
                            shardId,
                            primary,
                            primary ? primaryRecoverySource : PeerRecoverySource.INSTANCE,
                            unassignedInfo
                        )
                    );
                }
                shards[shardNumber] = indexShardRoutingBuilder;
            }
            return this;
        }

        public Builder addReplica() {
            assert shards != null;
            for (IndexShardRoutingTable.Builder existing : shards) {
                assert existing != null;
                // version 0, will get updated when reroute will happen
                existing.addShard(
                    ShardRouting.newUnassigned(
                        existing.shardId(),
                        false,
                        PeerRecoverySource.INSTANCE,
                        new UnassignedInfo(UnassignedInfo.Reason.REPLICA_ADDED, null)
                    )
                );
            }
            return this;
        }

        public Builder removeReplica() {
            assert shards != null;
            for (int shardId = 0; shardId < shards.length; shardId++) {
                IndexShardRoutingTable.Builder found = shards[shardId];
                assert found != null;
                final IndexShardRoutingTable indexShard = found.build();
                if (indexShard.replicaShards().isEmpty()) {
                    // nothing to do here!
                    return this;
                }
                // re-add all the current ones
                IndexShardRoutingTable.Builder builder = IndexShardRoutingTable.builder(indexShard.shardId());
                for (int copy = 0; copy < indexShard.size(); copy++) {
                    ShardRouting shardRouting = indexShard.shard(copy);
                    builder.addShard(shardRouting);
                }

                boolean removed = false;
                for (Predicate<ShardRouting> removeClause : PRIORITY_REMOVE_CLAUSES) {
                    if (removed == false) {
                        for (int copy = 0; copy < indexShard.size(); copy++) {
                            ShardRouting shardRouting = indexShard.shard(copy);
                            if (shardRouting.primary() == false && removeClause.test(shardRouting)) {
                                builder.removeShard(shardRouting);
                                removed = true;
                                break;
                            }
                        }
                    }
                }
                shards[shardId] = builder;
            }
            return this;
        }

        public Builder addIndexShard(IndexShardRoutingTable.Builder indexShard) {
            assert indexShard.shardId().getIndex().equals(index)
                : "cannot add shard routing table for " + indexShard.shardId() + " to index routing table for " + index;
            final int sid = indexShard.shardId().id();
            ensureShardArray(sid + 1);
            shards[sid] = indexShard;
            return this;
        }

        /**
         * Adds a new shard routing (makes a copy of it), with reference data used from the index shard routing table
         * if it needs to be created.
         */
        public Builder addShard(ShardRouting shard) {
            assert shard.index().equals(index) : "cannot add [" + shard + "] to routing table for " + index;
            int shardId = shard.id();
            ensureShardArray(shardId + 1);
            IndexShardRoutingTable.Builder indexShard = shards[shardId];
            if (indexShard == null) {
                shards[shardId] = IndexShardRoutingTable.builder(shard.shardId()).addShard(shard);
            } else {
                indexShard.addShard(shard);
            }
            return this;
        }

        void ensureShardArray(int shardCount) {
            if (shards == null) {
                shards = new IndexShardRoutingTable.Builder[shardCount];
            } else if (shards.length < shardCount) {
                IndexShardRoutingTable.Builder[] updated = new IndexShardRoutingTable.Builder[shardCount];
                System.arraycopy(shards, 0, updated, 0, shards.length);
                shards = updated;
            }
        }

        public IndexRoutingTable build() {
            final IndexShardRoutingTable[] res;
            if (shards != null) {
                res = new IndexShardRoutingTable[shards.length];
                for (int i = 0; i < shards.length; i++) {
                    res[i] = shards[i].build();
                }
            } else {
                res = new IndexShardRoutingTable[0];
            }
            return new IndexRoutingTable(index, res);
        }
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder("-- index [" + index + "]\n");
        for (IndexShardRoutingTable indexShard : shards) {
            sb.append("----shard_id [")
                .append(indexShard.shardId().getIndex().getName())
                .append("][")
                .append(indexShard.shardId().id())
                .append("]\n");
            for (int copy = 0; copy < indexShard.size(); copy++) {
                sb.append("--------").append(indexShard.shard(copy).shortSummary()).append("\n");
            }
        }
        return sb.toString();
    }

}
