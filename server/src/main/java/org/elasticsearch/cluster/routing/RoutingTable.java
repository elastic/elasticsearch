/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.isIndexVerifiedBeforeClosed;

/**
 * Represents a global cluster-wide routing table for all indices including the
 * version of the current routing state.
 *
 * @see IndexRoutingTable
 */
public class RoutingTable implements Iterable<IndexRoutingTable>, Diffable<RoutingTable> {

    public static final RoutingTable EMPTY_ROUTING_TABLE = builder().build();

    private final long version;

    // index to IndexRoutingTable map
    private final ImmutableOpenMap<String, IndexRoutingTable> indicesRouting;

    private RoutingTable(long version, ImmutableOpenMap<String, IndexRoutingTable> indicesRouting) {
        this.version = version;
        this.indicesRouting = indicesRouting;
    }

    /**
     * Get's the {@link IndexShardRoutingTable} for the given shard id from the given {@link IndexRoutingTable}
     * or throws a {@link ShardNotFoundException} if no shard by the given id is found in the IndexRoutingTable.
     *
     * @param indexRouting IndexRoutingTable
     * @param shardId ShardId
     * @return IndexShardRoutingTable
     */
    public static IndexShardRoutingTable shardRoutingTable(IndexRoutingTable indexRouting, int shardId) {
        IndexShardRoutingTable indexShard = indexRouting.shard(shardId);
        if (indexShard == null) {
            throw new ShardNotFoundException(new ShardId(indexRouting.getIndex(), shardId));
        }
        return indexShard;
    }

    /**
     * Returns the version of the {@link RoutingTable}.
     *
     * @return version of the {@link RoutingTable}
     */
    public long version() {
        return this.version;
    }

    @Override
    public Iterator<IndexRoutingTable> iterator() {
        return indicesRouting.valuesIt();
    }

    public boolean hasIndex(String index) {
        return indicesRouting.containsKey(index);
    }

    public boolean hasIndex(Index index) {
        IndexRoutingTable indexRouting = index(index);
        return indexRouting != null;
    }

    public IndexRoutingTable index(String index) {
        return indicesRouting.get(index);
    }

    public IndexRoutingTable index(Index index) {
        IndexRoutingTable indexRouting = index(index.getName());
        return indexRouting != null && indexRouting.getIndex().equals(index) ? indexRouting : null;
    }

    public ImmutableOpenMap<String, IndexRoutingTable> indicesRouting() {
        return indicesRouting;
    }

    public ImmutableOpenMap<String, IndexRoutingTable> getIndicesRouting() {
        return indicesRouting();
    }

    /**
     * All shards for the provided index and shard id
     * @return All the shard routing entries for the given index and shard id
     * @throws IndexNotFoundException if provided index does not exist
     * @throws ShardNotFoundException if provided shard id is unknown
     */
    public IndexShardRoutingTable shardRoutingTable(String index, int shardId) {
        IndexRoutingTable indexRouting = index(index);
        if (indexRouting == null) {
            throw new IndexNotFoundException(index);
        }
        return shardRoutingTable(indexRouting, shardId);
    }

    /**
     * All shards for the provided {@link ShardId}
     * @return All the shard routing entries for the given index and shard id
     * @throws IndexNotFoundException if provided index does not exist
     * @throws ShardNotFoundException if provided shard id is unknown
     */
    public IndexShardRoutingTable shardRoutingTable(ShardId shardId) {
        IndexRoutingTable indexRouting = index(shardId.getIndex());
        if (indexRouting == null) {
            throw new IndexNotFoundException(shardId.getIndex());
        }
        IndexShardRoutingTable shard = indexRouting.shard(shardId.id());
        if (shard == null) {
            throw new ShardNotFoundException(shardId);
        }
        return shard;
    }

    @Nullable
    public ShardRouting getByAllocationId(ShardId shardId, String allocationId) {
        final IndexRoutingTable indexRoutingTable = index(shardId.getIndex());
        if (indexRoutingTable == null) {
            return null;
        }
        final IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId.getId());
        return shardRoutingTable == null ? null : shardRoutingTable.getByAllocationId(allocationId);
    }

    public boolean validate(Metadata metadata) {
        for (IndexRoutingTable indexRoutingTable : this) {
            if (indexRoutingTable.validate(metadata) == false) {
                return false;
            }
        }
        return true;
    }

    public List<ShardRouting> shardsWithState(ShardRoutingState state) {
        List<ShardRouting> shards = new ArrayList<>();
        for (IndexRoutingTable indexRoutingTable : this) {
            shards.addAll(indexRoutingTable.shardsWithState(state));
        }
        return shards;
    }

    /**
     * All the shards (replicas) for all indices in this routing table.
     *
     * @return All the shards
     */
    public List<ShardRouting> allShards() {
        List<ShardRouting> shards = new ArrayList<>();
        String[] indices = indicesRouting.keys().toArray(String.class);
        for (String index : indices) {
            List<ShardRouting> allShardsIndex = allShards(index);
            shards.addAll(allShardsIndex);
        }
        return shards;
    }

    /**
     * All the shards (replicas) for the provided index.
     *
     * @param index The index to return all the shards (replicas).
     * @return All the shards matching the specific index
     * @throws IndexNotFoundException If the index passed does not exists
     */
    public List<ShardRouting> allShards(String index)  {
        List<ShardRouting> shards = new ArrayList<>();
        IndexRoutingTable indexRoutingTable = index(index);
        if (indexRoutingTable == null) {
            throw new IndexNotFoundException(index);
        }
        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                shards.add(shardRouting);
            }
        }
        return shards;
    }

    /**
     * Return GroupShardsIterator where each active shard routing has it's own shard iterator.
     *
     * @param includeEmpty             if true, a shard iterator will be added for non-assigned shards as well
     */
    public GroupShardsIterator<ShardIterator> allActiveShardsGrouped(String[] indices, boolean includeEmpty) {
        return allSatisfyingPredicateShardsGrouped(indices, includeEmpty, ACTIVE_PREDICATE);
    }

    /**
     * Return GroupShardsIterator where each assigned shard routing has it's own shard iterator.
     *
     * @param includeEmpty if true, a shard iterator will be added for non-assigned shards as well
     */
    public GroupShardsIterator<ShardIterator> allAssignedShardsGrouped(String[] indices, boolean includeEmpty) {
        return allSatisfyingPredicateShardsGrouped(indices, includeEmpty, ASSIGNED_PREDICATE);
    }

    private static Predicate<ShardRouting> ACTIVE_PREDICATE = ShardRouting::active;
    private static Predicate<ShardRouting> ASSIGNED_PREDICATE = ShardRouting::assignedToNode;

    private GroupShardsIterator<ShardIterator> allSatisfyingPredicateShardsGrouped(String[] indices, boolean includeEmpty,
                                                                                   Predicate<ShardRouting> predicate) {
        // use list here since we need to maintain identity across shards
        ArrayList<ShardIterator> set = new ArrayList<>();
        for (String index : indices) {
            IndexRoutingTable indexRoutingTable = index(index);
            if (indexRoutingTable == null) {
                continue;
                // we simply ignore indices that don't exists (make sense for operations that use it currently)
            }
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    if (predicate.test(shardRouting)) {
                        set.add(shardRouting.shardsIt());
                    } else if (includeEmpty) { // we need this for counting properly, just make it an empty one
                        set.add(new PlainShardIterator(shardRouting.shardId(), Collections.emptyList()));
                    }
                }
            }
        }
        return GroupShardsIterator.sortAndCreate(set);
    }

    public ShardsIterator allShards(String[] indices) {
        return allShardsSatisfyingPredicate(indices, shardRouting -> true, false);
    }

    public ShardsIterator allActiveShards(String[] indices) {
        return allShardsSatisfyingPredicate(indices, ShardRouting::active, false);
    }

    public ShardsIterator allShardsIncludingRelocationTargets(String[] indices) {
        return allShardsSatisfyingPredicate(indices, shardRouting -> true, true);
    }

    private ShardsIterator allShardsSatisfyingPredicate(String[] indices, Predicate<ShardRouting> predicate,
                                                        boolean includeRelocationTargets) {
        // use list here since we need to maintain identity across shards
        List<ShardRouting> shards = new ArrayList<>();
        for (String index : indices) {
            IndexRoutingTable indexRoutingTable = index(index);
            if (indexRoutingTable == null) {
                continue;
                // we simply ignore indices that don't exists (make sense for operations that use it currently)
            }
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    if (predicate.test(shardRouting)) {
                        shards.add(shardRouting);
                        if (includeRelocationTargets && shardRouting.relocating()) {
                            shards.add(shardRouting.getTargetRelocatingShard());
                        }
                    }
                }
            }
        }
        return new PlainShardsIterator(shards);
    }

    /**
     * All the *active* primary shards for the provided indices grouped (each group is a single element, consisting
     * of the primary shard). This is handy for components that expect to get group iterators, but still want in some
     * cases to iterate over all primary shards (and not just one shard in replication group).
     *
     * @param indices The indices to return all the shards (replicas)
     * @return All the primary shards grouped into a single shard element group each
     * @throws IndexNotFoundException If an index passed does not exists
     */
    public GroupShardsIterator<ShardIterator> activePrimaryShardsGrouped(String[] indices, boolean includeEmpty) {
        // use list here since we need to maintain identity across shards
        ArrayList<ShardIterator> set = new ArrayList<>();
        for (String index : indices) {
            IndexRoutingTable indexRoutingTable = index(index);
            if (indexRoutingTable == null) {
                throw new IndexNotFoundException(index);
            }
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                ShardRouting primary = indexShardRoutingTable.primaryShard();
                if (primary.active()) {
                    set.add(primary.shardsIt());
                } else if (includeEmpty) { // we need this for counting properly, just make it an empty one
                    set.add(new PlainShardIterator(primary.shardId(), Collections.emptyList()));
                }
            }
        }
        return GroupShardsIterator.sortAndCreate(set);
    }

    @Override
    public Diff<RoutingTable> diff(RoutingTable previousState) {
        return new RoutingTableDiff(previousState, this);
    }

    public static Diff<RoutingTable> readDiffFrom(StreamInput in) throws IOException {
        return new RoutingTableDiff(in);
    }

    public static RoutingTable readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder();
        builder.version = in.readLong();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            IndexRoutingTable index = IndexRoutingTable.readFrom(in);
            builder.add(index);
        }

        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeVInt(indicesRouting.size());
        for (ObjectCursor<IndexRoutingTable> index : indicesRouting.values()) {
            index.value.writeTo(out);
        }
    }

    private static class RoutingTableDiff implements Diff<RoutingTable> {

        private final long version;

        private final Diff<ImmutableOpenMap<String, IndexRoutingTable>> indicesRouting;

        RoutingTableDiff(RoutingTable before, RoutingTable after) {
            version = after.version;
            indicesRouting = DiffableUtils.diff(before.indicesRouting, after.indicesRouting, DiffableUtils.getStringKeySerializer());
        }

        private static final DiffableUtils.DiffableValueReader<String, IndexRoutingTable> DIFF_VALUE_READER =
                new DiffableUtils.DiffableValueReader<>(IndexRoutingTable::readFrom, IndexRoutingTable::readDiffFrom);

        RoutingTableDiff(StreamInput in) throws IOException {
            version = in.readLong();
            indicesRouting = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), DIFF_VALUE_READER);
        }

        @Override
        public RoutingTable apply(RoutingTable part) {
            return new RoutingTable(version, indicesRouting.apply(part.indicesRouting));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(version);
            indicesRouting.writeTo(out);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(RoutingTable routingTable) {
        return new Builder(routingTable);
    }

    /**
     * Builder for the routing table. Note that build can only be called one time.
     */
    public static class Builder {

        private long version;
        private ImmutableOpenMap.Builder<String, IndexRoutingTable> indicesRouting = ImmutableOpenMap.builder();

        public Builder() {

        }

        public Builder(RoutingTable routingTable) {
            version = routingTable.version;
            for (IndexRoutingTable indexRoutingTable : routingTable) {
                indicesRouting.put(indexRoutingTable.getIndex().getName(), indexRoutingTable);
            }
        }

        public Builder updateNodes(long version, RoutingNodes routingNodes) {
            // this is being called without pre initializing the routing table, so we must copy over the version as well
            this.version = version;

            Map<String, IndexRoutingTable.Builder> indexRoutingTableBuilders = new HashMap<>();
            for (RoutingNode routingNode : routingNodes) {
                for (ShardRouting shardRoutingEntry : routingNode) {
                    // every relocating shard has a double entry, ignore the target one.
                    if (shardRoutingEntry.initializing() && shardRoutingEntry.relocatingNodeId() != null)
                        continue;

                    addShard(indexRoutingTableBuilders, shardRoutingEntry);
                }
            }

            Iterable<ShardRouting> shardRoutingEntries = Iterables.concat(routingNodes.unassigned(), routingNodes.unassigned().ignored());

            for (ShardRouting shardRoutingEntry : shardRoutingEntries) {
                addShard(indexRoutingTableBuilders, shardRoutingEntry);
            }

            for (IndexRoutingTable.Builder indexBuilder : indexRoutingTableBuilders.values()) {
                add(indexBuilder);
            }
            return this;
        }

        private static void addShard(final Map<String, IndexRoutingTable.Builder> indexRoutingTableBuilders,
                final ShardRouting shardRoutingEntry) {
            Index index = shardRoutingEntry.index();
            IndexRoutingTable.Builder indexBuilder = indexRoutingTableBuilders.get(index.getName());
            if (indexBuilder == null) {
                indexBuilder = new IndexRoutingTable.Builder(index);
                indexRoutingTableBuilders.put(index.getName(), indexBuilder);
            }
            indexBuilder.addShard(shardRoutingEntry);
        }

        /**
         * Update the number of replicas for the specified indices.
         *
         * @param numberOfReplicas the number of replicas
         * @param indices          the indices to update the number of replicas for
         * @return the builder
         */
        public Builder updateNumberOfReplicas(final int numberOfReplicas, final String[] indices) {
            if (indicesRouting == null) {
                throw new IllegalStateException("once build is called the builder cannot be reused");
            }
            for (String index : indices) {
                IndexRoutingTable indexRoutingTable = indicesRouting.get(index);
                if (indexRoutingTable == null) {
                    // ignore index missing failure, its closed...
                    continue;
                }
                int currentNumberOfReplicas = indexRoutingTable.shards().get(0).size() - 1; // remove the required primary
                IndexRoutingTable.Builder builder = new IndexRoutingTable.Builder(indexRoutingTable.getIndex());
                // re-add all the shards
                for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                    builder.addIndexShard(indexShardRoutingTable);
                }
                if (currentNumberOfReplicas < numberOfReplicas) {
                    // now, add "empty" ones
                    for (int i = 0; i < (numberOfReplicas - currentNumberOfReplicas); i++) {
                        builder.addReplica();
                    }
                } else if (currentNumberOfReplicas > numberOfReplicas) {
                    for (int i = 0; i < (currentNumberOfReplicas - numberOfReplicas); i++) {
                        builder.removeReplica();
                    }
                }
                indicesRouting.put(index, builder.build());
            }
            return this;
        }

        public Builder addAsNew(IndexMetadata indexMetadata) {
            if (indexMetadata.getState() == IndexMetadata.State.OPEN) {
                IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(indexMetadata.getIndex())
                        .initializeAsNew(indexMetadata);
                add(indexRoutingBuilder);
            }
            return this;
        }

        public Builder addAsRecovery(IndexMetadata indexMetadata) {
            if (indexMetadata.getState() == IndexMetadata.State.OPEN || isIndexVerifiedBeforeClosed(indexMetadata)) {
                IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(indexMetadata.getIndex())
                    .initializeAsRecovery(indexMetadata);
                add(indexRoutingBuilder);
            }
            return this;
        }

        public Builder addAsFromDangling(IndexMetadata indexMetadata) {
            if (indexMetadata.getState() == IndexMetadata.State.OPEN || isIndexVerifiedBeforeClosed(indexMetadata)) {
                IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(indexMetadata.getIndex())
                        .initializeAsFromDangling(indexMetadata);
                add(indexRoutingBuilder);
            }
            return this;
        }

        public Builder addAsFromCloseToOpen(IndexMetadata indexMetadata) {
            if (indexMetadata.getState() == IndexMetadata.State.OPEN) {
                IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(indexMetadata.getIndex())
                        .initializeAsFromCloseToOpen(indexMetadata);
                add(indexRoutingBuilder);
            }
            return this;
        }

        public Builder addAsFromOpenToClose(IndexMetadata indexMetadata) {
            assert isIndexVerifiedBeforeClosed(indexMetadata);
            IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(indexMetadata.getIndex())
                .initializeAsFromOpenToClose(indexMetadata);
            return add(indexRoutingBuilder);
        }

        public Builder addAsRestore(IndexMetadata indexMetadata, SnapshotRecoverySource recoverySource) {
            IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(indexMetadata.getIndex())
                    .initializeAsRestore(indexMetadata, recoverySource);
            add(indexRoutingBuilder);
            return this;
        }

        public Builder addAsNewRestore(IndexMetadata indexMetadata, SnapshotRecoverySource recoverySource, IntSet ignoreShards) {
            IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(indexMetadata.getIndex())
                    .initializeAsNewRestore(indexMetadata, recoverySource, ignoreShards);
            add(indexRoutingBuilder);
            return this;
        }

        public Builder add(IndexRoutingTable indexRoutingTable) {
            if (indicesRouting == null) {
                throw new IllegalStateException("once build is called the builder cannot be reused");
            }
            indicesRouting.put(indexRoutingTable.getIndex().getName(), indexRoutingTable);
            return this;
        }

        public Builder add(IndexRoutingTable.Builder indexRoutingTableBuilder) {
            add(indexRoutingTableBuilder.build());
            return this;
        }

        public Builder remove(String index) {
            if (indicesRouting == null) {
                throw new IllegalStateException("once build is called the builder cannot be reused");
            }
            indicesRouting.remove(index);
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        /**
         * Builds the routing table. Note that once this is called the builder
         * must be thrown away. If you need to build a new RoutingTable as a
         * copy of this one you'll need to build a new RoutingTable.Builder.
         */
        public RoutingTable build() {
            if (indicesRouting == null) {
                throw new IllegalStateException("once build is called the builder cannot be reused");
            }
            RoutingTable table = new RoutingTable(version, indicesRouting.build());
            indicesRouting = null;
            return table;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("routing_table (version ").append(version).append("):\n");
        for (ObjectObjectCursor<String, IndexRoutingTable> entry : indicesRouting) {
            sb.append(entry.value.prettyPrint()).append('\n');
        }
        return sb.toString();
    }

}
