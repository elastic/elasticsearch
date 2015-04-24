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

package org.elasticsearch.cluster.routing;

import com.carrotsearch.hppc.IntSet;
import com.google.common.collect.*;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

/**
 * Represents a global cluster-wide routing table for all indices including the
 * version of the current routing state.
 *
 * @see IndexRoutingTable
 */
public class RoutingTable implements Iterable<IndexRoutingTable> {

    public static final RoutingTable EMPTY_ROUTING_TABLE = builder().build();

    private final long version;

    // index to IndexRoutingTable map
    private final ImmutableMap<String, IndexRoutingTable> indicesRouting;

    RoutingTable(long version, Map<String, IndexRoutingTable> indicesRouting) {
        this.version = version;
        this.indicesRouting = ImmutableMap.copyOf(indicesRouting);
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
    public UnmodifiableIterator<IndexRoutingTable> iterator() {
        return indicesRouting.values().iterator();
    }

    public boolean hasIndex(String index) {
        return indicesRouting.containsKey(index);
    }

    public IndexRoutingTable index(String index) {
        return indicesRouting.get(index);
    }

    public Map<String, IndexRoutingTable> indicesRouting() {
        return indicesRouting;
    }

    public Map<String, IndexRoutingTable> getIndicesRouting() {
        return indicesRouting();
    }

    public RoutingNodes routingNodes(ClusterState state) {
        return new RoutingNodes(state);
    }

    public RoutingTable validateRaiseException(MetaData metaData) throws RoutingValidationException {
        RoutingTableValidation validation = validate(metaData);
        if (!validation.valid()) {
            throw new RoutingValidationException(validation);
        }
        return this;
    }

    public RoutingTableValidation validate(MetaData metaData) {
        RoutingTableValidation validation = new RoutingTableValidation();
        for (IndexRoutingTable indexRoutingTable : this) {
            indexRoutingTable.validate(validation, metaData);
        }
        return validation;
    }

    public List<ShardRouting> shardsWithState(ShardRoutingState state) {
        List<ShardRouting> shards = newArrayList();
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
    public List<ShardRouting> allShards() throws IndexMissingException {
        List<ShardRouting> shards = Lists.newArrayList();
        String[] indices = indicesRouting.keySet().toArray(new String[indicesRouting.keySet().size()]);
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
     * @throws IndexMissingException If the index passed does not exists
     */
    public List<ShardRouting> allShards(String index) throws IndexMissingException {
        List<ShardRouting> shards = Lists.newArrayList();
        IndexRoutingTable indexRoutingTable = index(index);
        if (indexRoutingTable == null) {
            throw new IndexMissingException(new Index(index));
        }
        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                shards.add(shardRouting);
            }
        }
        return shards;
    }

    public GroupShardsIterator allActiveShardsGrouped(String[] indices, boolean includeEmpty) throws IndexMissingException {
        return allActiveShardsGrouped(indices, includeEmpty, false);
    }

    /**
     * Return GroupShardsIterator where each active shard routing has it's own shard iterator.
     *
     * @param includeEmpty             if true, a shard iterator will be added for non-assigned shards as well
     * @param includeRelocationTargets if true, an <b>extra</b> shard iterator will be added for relocating shards. The extra
     *                                 iterator contains a single ShardRouting pointing at the relocating target
     */
    public GroupShardsIterator allActiveShardsGrouped(String[] indices, boolean includeEmpty, boolean includeRelocationTargets) throws IndexMissingException {
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
                    if (shardRouting.active()) {
                        set.add(shardRouting.shardsIt());
                        if (includeRelocationTargets && shardRouting.relocating()) {
                            set.add(new PlainShardIterator(shardRouting.shardId(), ImmutableList.of(shardRouting.targetRoutingIfRelocating())));
                        }
                    } else if (includeEmpty) { // we need this for counting properly, just make it an empty one
                        set.add(new PlainShardIterator(shardRouting.shardId(), ImmutableList.<ShardRouting>of()));
                    }
                }
            }
        }
        return new GroupShardsIterator(set);
    }

    public GroupShardsIterator allAssignedShardsGrouped(String[] indices, boolean includeEmpty) throws IndexMissingException {
        return allAssignedShardsGrouped(indices, includeEmpty, false);
    }

    /**
     * Return GroupShardsIterator where each assigned shard routing has it's own shard iterator.
     *
     * @param includeEmpty if true, a shard iterator will be added for non-assigned shards as well
     * @param includeRelocationTargets if true, an <b>extra</b> shard iterator will be added for relocating shards. The extra
     *                                 iterator contains a single ShardRouting pointing at the relocating target
     */
    public GroupShardsIterator allAssignedShardsGrouped(String[] indices, boolean includeEmpty, boolean includeRelocationTargets) throws IndexMissingException {
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
                    if (shardRouting.assignedToNode()) {
                        set.add(shardRouting.shardsIt());
                        if (includeRelocationTargets && shardRouting.relocating()) {
                            set.add(new PlainShardIterator(shardRouting.shardId(), ImmutableList.of(shardRouting.targetRoutingIfRelocating())));
                        }
                    } else if (includeEmpty) { // we need this for counting properly, just make it an empty one
                        set.add(new PlainShardIterator(shardRouting.shardId(), ImmutableList.<ShardRouting>of()));
                    }
                }
            }
        }
        return new GroupShardsIterator(set);
    }

    /**
     * All the *active* primary shards for the provided indices grouped (each group is a single element, consisting
     * of the primary shard). This is handy for components that expect to get group iterators, but still want in some
     * cases to iterate over all primary shards (and not just one shard in replication group).
     *
     * @param indices The indices to return all the shards (replicas)
     * @return All the primary shards grouped into a single shard element group each
     * @throws IndexMissingException If an index passed does not exists
     * @see IndexRoutingTable#groupByAllIt()
     */
    public GroupShardsIterator activePrimaryShardsGrouped(String[] indices, boolean includeEmpty) throws IndexMissingException {
        // use list here since we need to maintain identity across shards
        ArrayList<ShardIterator> set = new ArrayList<>();
        for (String index : indices) {
            IndexRoutingTable indexRoutingTable = index(index);
            if (indexRoutingTable == null) {
                throw new IndexMissingException(new Index(index));
            }
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                ShardRouting primary = indexShardRoutingTable.primaryShard();
                if (primary.active()) {
                    set.add(primary.shardsIt());
                } else if (includeEmpty) { // we need this for counting properly, just make it an empty one
                    set.add(new PlainShardIterator(primary.shardId(), ImmutableList.<ShardRouting>of()));
                }
            }
        }
        return new GroupShardsIterator(set);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(RoutingTable routingTable) {
        return new Builder(routingTable);
    }

    public static class Builder {

        private long version;
        private final Map<String, IndexRoutingTable> indicesRouting = newHashMap();

        public Builder() {

        }

        public Builder(RoutingTable routingTable) {
            version = routingTable.version;
            for (IndexRoutingTable indexRoutingTable : routingTable) {
                indicesRouting.put(indexRoutingTable.index(), indexRoutingTable);
            }
        }

        public Builder updateNodes(RoutingNodes routingNodes) {
            // this is being called without pre initializing the routing table, so we must copy over the version as well
            this.version = routingNodes.routingTable().version();

            Map<String, IndexRoutingTable.Builder> indexRoutingTableBuilders = newHashMap();
            for (RoutingNode routingNode : routingNodes) {
                for (MutableShardRouting shardRoutingEntry : routingNode) {
                    // every relocating shard has a double entry, ignore the target one.
                    if (shardRoutingEntry.state() == ShardRoutingState.INITIALIZING && shardRoutingEntry.relocatingNodeId() != null)
                        continue;

                    String index = shardRoutingEntry.index();
                    IndexRoutingTable.Builder indexBuilder = indexRoutingTableBuilders.get(index);
                    if (indexBuilder == null) {
                        indexBuilder = new IndexRoutingTable.Builder(index);
                        indexRoutingTableBuilders.put(index, indexBuilder);
                    }

                    IndexShardRoutingTable refData = routingNodes.routingTable().index(shardRoutingEntry.index()).shard(shardRoutingEntry.id());
                    indexBuilder.addShard(refData, shardRoutingEntry);
                }
            }
            for (MutableShardRouting shardRoutingEntry : Iterables.concat(routingNodes.unassigned(), routingNodes.ignoredUnassigned())) {
                String index = shardRoutingEntry.index();
                IndexRoutingTable.Builder indexBuilder = indexRoutingTableBuilders.get(index);
                if (indexBuilder == null) {
                    indexBuilder = new IndexRoutingTable.Builder(index);
                    indexRoutingTableBuilders.put(index, indexBuilder);
                }
                IndexShardRoutingTable refData = routingNodes.routingTable().index(shardRoutingEntry.index()).shard(shardRoutingEntry.id());
                indexBuilder.addShard(refData, shardRoutingEntry);
            }

            for (ShardId shardId : routingNodes.getShardsToClearPostAllocationFlag()) {
                IndexRoutingTable.Builder indexRoutingBuilder = indexRoutingTableBuilders.get(shardId.index().name());
                if (indexRoutingBuilder != null) {
                    indexRoutingBuilder.clearPostAllocationFlag(shardId);
                }
            }

            for (IndexRoutingTable.Builder indexBuilder : indexRoutingTableBuilders.values()) {
                add(indexBuilder);
            }
            return this;
        }

        public Builder updateNumberOfReplicas(int numberOfReplicas, String... indices) throws IndexMissingException {
            if (indices == null || indices.length == 0) {
                indices = indicesRouting.keySet().toArray(new String[indicesRouting.keySet().size()]);
            }
            for (String index : indices) {
                IndexRoutingTable indexRoutingTable = indicesRouting.get(index);
                if (indexRoutingTable == null) {
                    // ignore index missing failure, its closed...
                    continue;
                }
                int currentNumberOfReplicas = indexRoutingTable.shards().get(0).size() - 1; // remove the required primary
                IndexRoutingTable.Builder builder = new IndexRoutingTable.Builder(index);
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
                    int delta = currentNumberOfReplicas - numberOfReplicas;
                    if (delta <= 0) {
                        // ignore, can't remove below the current one...
                    } else {
                        for (int i = 0; i < delta; i++) {
                            builder.removeReplica();
                        }
                    }
                }
                indicesRouting.put(index, builder.build());
            }
            return this;
        }

        public Builder addAsNew(IndexMetaData indexMetaData) {
            if (indexMetaData.state() == IndexMetaData.State.OPEN) {
                IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(indexMetaData.index())
                        .initializeAsNew(indexMetaData);
                add(indexRoutingBuilder);
            }
            return this;
        }

        public Builder addAsRecovery(IndexMetaData indexMetaData) {
            if (indexMetaData.state() == IndexMetaData.State.OPEN) {
                IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(indexMetaData.index())
                        .initializeAsRecovery(indexMetaData);
                add(indexRoutingBuilder);
            }
            return this;
        }

        public Builder addAsRestore(IndexMetaData indexMetaData, RestoreSource restoreSource) {
            IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(indexMetaData.index())
                    .initializeAsRestore(indexMetaData, restoreSource);
            add(indexRoutingBuilder);
            return this;
        }

        public Builder addAsNewRestore(IndexMetaData indexMetaData, RestoreSource restoreSource, IntSet ignoreShards) {
            IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(indexMetaData.index())
                    .initializeAsNewRestore(indexMetaData, restoreSource, ignoreShards);
            add(indexRoutingBuilder);
            return this;
        }

        public Builder add(IndexRoutingTable indexRoutingTable) {
            indexRoutingTable.validate();
            indicesRouting.put(indexRoutingTable.index(), indexRoutingTable);
            return this;
        }

        public Builder add(IndexRoutingTable.Builder indexRoutingTableBuilder) {
            add(indexRoutingTableBuilder.build());
            return this;
        }

        public Builder remove(String index) {
            indicesRouting.remove(index);
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public RoutingTable build() {
            // normalize the versions right before we build it...
            for (IndexRoutingTable indexRoutingTable : indicesRouting.values()) {
                indicesRouting.put(indexRoutingTable.index(), indexRoutingTable.normalizeVersions());
            }
            return new RoutingTable(version, indicesRouting);
        }

        public static RoutingTable readFrom(StreamInput in) throws IOException {
            Builder builder = new Builder();
            builder.version = in.readLong();
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                IndexRoutingTable index = IndexRoutingTable.Builder.readFrom(in);
                builder.add(index);
            }

            return builder.build();
        }

        public static void writeTo(RoutingTable table, StreamOutput out) throws IOException {
            out.writeLong(table.version);
            out.writeVInt(table.indicesRouting.size());
            for (IndexRoutingTable index : table.indicesRouting.values()) {
                IndexRoutingTable.Builder.writeTo(index, out);
            }
        }
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder("routing_table (version ").append(version).append("):\n");
        for (Map.Entry<String, IndexRoutingTable> entry : indicesRouting.entrySet()) {
            sb.append(entry.getValue().prettyPrint()).append('\n');
        }
        return sb.toString();
    }


}
