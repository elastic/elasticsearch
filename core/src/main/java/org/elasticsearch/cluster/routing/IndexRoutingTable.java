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
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The {@link IndexRoutingTable} represents routing information for a single
 * index. The routing table maintains a list of all shards in the index. A
 * single shard in this context has one more instances namely exactly one
 * {@link ShardRouting#primary() primary} and 1 or more replicas. In other
 * words, each instance of a shard is considered a replica while only one
 * replica per shard is a <tt>primary</tt> replica. The <tt>primary</tt> replica
 * can be seen as the "leader" of the shard acting as the primary entry point
 * for operations on a specific shard.
 * <p>
 * Note: The term replica is not directly
 * reflected in the routing table or in releated classes, replicas are
 * represented as {@link ShardRouting}.
 * </p>
 */
public class IndexRoutingTable extends AbstractDiffable<IndexRoutingTable> implements Iterable<IndexShardRoutingTable> {

    public static final IndexRoutingTable PROTO = builder("").build();

    private final String index;
    private final ShardShuffler shuffler;

    // note, we assume that when the index routing is created, ShardRoutings are created for all possible number of
    // shards with state set to UNASSIGNED
    private final ImmutableOpenIntMap<IndexShardRoutingTable> shards;

    private final List<ShardRouting> allActiveShards;

    IndexRoutingTable(String index, ImmutableOpenIntMap<IndexShardRoutingTable> shards) {
        this.index = index;
        this.shuffler = new RotationShardShuffler(ThreadLocalRandom.current().nextInt());
        this.shards = shards;
        List<ShardRouting> allActiveShards = new ArrayList<>();
        for (IntObjectCursor<IndexShardRoutingTable> cursor : shards) {
            for (ShardRouting shardRouting : cursor.value) {
                shardRouting.freeze();
                if (shardRouting.active()) {
                    allActiveShards.add(shardRouting);
                }
            }
        }
        this.allActiveShards = Collections.unmodifiableList(allActiveShards);
    }

    /**
     * Return the index id
     *
     * @return id of the index
     */
    public String index() {
        return this.index;
    }


    /**
     * Return the index id
     *
     * @return id of the index
     */
    public String getIndex() {
        return index();
    }

    /**
     * creates a new {@link IndexRoutingTable} with all shard versions normalized
     *
     * @return new {@link IndexRoutingTable}
     */
    public IndexRoutingTable normalizeVersions() {
        IndexRoutingTable.Builder builder = new Builder(this.index);
        for (IntObjectCursor<IndexShardRoutingTable> cursor : shards) {
            builder.addIndexShard(cursor.value.normalizeVersions());
        }
        return builder.build();
    }

    public void validate(RoutingTableValidation validation, MetaData metaData) {
        if (!metaData.hasIndex(index())) {
            validation.addIndexFailure(index(), "Exists in routing does not exists in metadata");
            return;
        }
        IndexMetaData indexMetaData = metaData.index(index());
        for (String failure : validate(indexMetaData)) {
            validation.addIndexFailure(index, failure);
        }

    }

    /**
     * validate based on a meta data, returning failures found
     */
    public List<String> validate(IndexMetaData indexMetaData) {
        ArrayList<String> failures = new ArrayList<>();

        // check the number of shards
        if (indexMetaData.getNumberOfShards() != shards().size()) {
            Set<Integer> expected = new HashSet<>();
            for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
                expected.add(i);
            }
            for (IndexShardRoutingTable indexShardRoutingTable : this) {
                expected.remove(indexShardRoutingTable.shardId().id());
            }
            failures.add("Wrong number of shards in routing table, missing: " + expected);
        }
        // check the replicas
        for (IndexShardRoutingTable indexShardRoutingTable : this) {
            int routingNumberOfReplicas = indexShardRoutingTable.size() - 1;
            if (routingNumberOfReplicas != indexMetaData.getNumberOfReplicas()) {
                failures.add("Shard [" + indexShardRoutingTable.shardId().id()
                        + "] routing table has wrong number of replicas, expected [" + indexMetaData.getNumberOfReplicas() + "], got [" + routingNumberOfReplicas + "]");
            }
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                if (!shardRouting.index().equals(index())) {
                    failures.add("shard routing has an index [" + shardRouting.index() + "] that is different than the routing table");
                }
            }
        }
        return failures;
    }

    @Override
    public Iterator<IndexShardRoutingTable> iterator() {
        return shards.valuesIt();
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
        for (IndexShardRoutingTable shardRoutingTable : this) {
            for (ShardRouting shardRouting : shardRoutingTable) {
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
                    if (!excluded) {
                        nodes.add(currentNodeId);
                    }
                }
            }
        }
        return nodes.size();
    }

    public ImmutableOpenIntMap<IndexShardRoutingTable> shards() {
        return shards;
    }

    public ImmutableOpenIntMap<IndexShardRoutingTable> getShards() {
        return shards();
    }

    public IndexShardRoutingTable shard(int shardId) {
        return shards.get(shardId);
    }

    /**
     * Returns <code>true</code> if all shards are primary and active. Otherwise <code>false</code>.
     */
    public boolean allPrimaryShardsActive() {
        return primaryShardsActive() == shards().size();
    }

    /**
     * Calculates the number of primary shards in active state in routing table
     *
     * @return number of active primary shards
     */
    public int primaryShardsActive() {
        int counter = 0;
        for (IndexShardRoutingTable shardRoutingTable : this) {
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
        return primaryShardsUnassigned() == shards.size();
    }

    /**
     * Calculates the number of primary shards in the routing table the are in
     * {@link ShardRoutingState#UNASSIGNED} state.
     */
    public int primaryShardsUnassigned() {
        int counter = 0;
        for (IndexShardRoutingTable shardRoutingTable : this) {
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
        for (IndexShardRoutingTable shardRoutingTable : this) {
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

    /**
     * A group shards iterator where each group ({@link ShardIterator}
     * is an iterator across shard replication group.
     */
    public GroupShardsIterator groupByShardsIt() {
        // use list here since we need to maintain identity across shards
        ArrayList<ShardIterator> set = new ArrayList<>(shards.size());
        for (IndexShardRoutingTable indexShard : this) {
            set.add(indexShard.shardsIt());
        }
        return new GroupShardsIterator(set);
    }

    /**
     * A groups shards iterator where each groups is a single {@link ShardRouting} and a group
     * is created for each shard routing.
     * <p>
     * This basically means that components that use the {@link GroupShardsIterator} will iterate
     * over *all* the shards (all the replicas) within the index.</p>
     */
    public GroupShardsIterator groupByAllIt() {
        // use list here since we need to maintain identity across shards
        ArrayList<ShardIterator> set = new ArrayList<>();
        for (IndexShardRoutingTable indexShard : this) {
            for (ShardRouting shardRouting : indexShard) {
                set.add(shardRouting.shardsIt());
            }
        }
        return new GroupShardsIterator(set);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexRoutingTable that = (IndexRoutingTable) o;

        if (!index.equals(that.index)) return false;
        if (!shards.equals(that.shards)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + shards.hashCode();
        return result;
    }

    public void validate() throws RoutingValidationException {
    }

    @Override
    public IndexRoutingTable readFrom(StreamInput in) throws IOException {
        String index = in.readString();
        Builder builder = new Builder(index);

        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.addIndexShard(IndexShardRoutingTable.Builder.readFromThin(in, index));
        }

        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shards.size());
        for (IndexShardRoutingTable indexShard : this) {
            IndexShardRoutingTable.Builder.writeToThin(indexShard, out);
        }
    }

    public static Builder builder(String index) {
        return new Builder(index);
    }

    public static class Builder {

        private final String index;
        private final ImmutableOpenIntMap.Builder<IndexShardRoutingTable> shards = ImmutableOpenIntMap.builder();

        public Builder(String index) {
            this.index = index;
        }

        /**
         * Reads an {@link IndexRoutingTable} from an {@link StreamInput}
         *
         * @param in {@link StreamInput} to read the {@link IndexRoutingTable} from
         * @return {@link IndexRoutingTable} read
         * @throws IOException if something happens during read
         */
        public static IndexRoutingTable readFrom(StreamInput in) throws IOException {
            return PROTO.readFrom(in);
        }

        /**
         * Initializes a new empty index, as if it was created from an API.
         */
        public Builder initializeAsNew(IndexMetaData indexMetaData) {
            return initializeEmpty(indexMetaData, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        }

        /**
         * Initializes a new empty index, as if it was created from an API.
         */
        public Builder initializeAsRecovery(IndexMetaData indexMetaData) {
            return initializeEmpty(indexMetaData, new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null));
        }

        /**
         * Initializes a new index caused by dangling index imported.
         */
        public Builder initializeAsFromDangling(IndexMetaData indexMetaData) {
            return initializeEmpty(indexMetaData, new UnassignedInfo(UnassignedInfo.Reason.DANGLING_INDEX_IMPORTED, null));
        }

        /**
         * Initializes a new empty index, as as a result of opening a closed index.
         */
        public Builder initializeAsFromCloseToOpen(IndexMetaData indexMetaData) {
            return initializeEmpty(indexMetaData, new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, null));
        }

        /**
         * Initializes a new empty index, to be restored from a snapshot
         */
        public Builder initializeAsNewRestore(IndexMetaData indexMetaData, RestoreSource restoreSource, IntSet ignoreShards) {
            return initializeAsRestore(indexMetaData, restoreSource, ignoreShards, true, new UnassignedInfo(UnassignedInfo.Reason.NEW_INDEX_RESTORED, "restore_source[" + restoreSource.snapshotId().getRepository() + "/" + restoreSource.snapshotId().getSnapshot() + "]"));
        }

        /**
         * Initializes an existing index, to be restored from a snapshot
         */
        public Builder initializeAsRestore(IndexMetaData indexMetaData, RestoreSource restoreSource) {
            return initializeAsRestore(indexMetaData, restoreSource, null, false, new UnassignedInfo(UnassignedInfo.Reason.EXISTING_INDEX_RESTORED, "restore_source[" + restoreSource.snapshotId().getRepository() + "/" + restoreSource.snapshotId().getSnapshot() + "]"));
        }

        /**
         * Initializes an index, to be restored from snapshot
         */
        private Builder initializeAsRestore(IndexMetaData indexMetaData, RestoreSource restoreSource, IntSet ignoreShards, boolean asNew, UnassignedInfo unassignedInfo) {
            if (!shards.isEmpty()) {
                throw new IllegalStateException("trying to initialize an index with fresh shards, but already has shards created");
            }
            for (int shardId = 0; shardId < indexMetaData.getNumberOfShards(); shardId++) {
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(new ShardId(indexMetaData.getIndex(), shardId));
                for (int i = 0; i <= indexMetaData.getNumberOfReplicas(); i++) {
                    if (asNew && ignoreShards.contains(shardId)) {
                        // This shards wasn't completely snapshotted - restore it as new shard
                        indexShardRoutingBuilder.addShard(ShardRouting.newUnassigned(index, shardId, null, i == 0, unassignedInfo));
                    } else {
                        indexShardRoutingBuilder.addShard(ShardRouting.newUnassigned(index, shardId, i == 0 ? restoreSource : null, i == 0, unassignedInfo));
                    }
                }
                shards.put(shardId, indexShardRoutingBuilder.build());
            }
            return this;
        }

        /**
         * Initializes a new empty index, with an option to control if its from an API or not.
         */
        private Builder initializeEmpty(IndexMetaData indexMetaData, UnassignedInfo unassignedInfo) {
            if (!shards.isEmpty()) {
                throw new IllegalStateException("trying to initialize an index with fresh shards, but already has shards created");
            }
            for (int shardId = 0; shardId < indexMetaData.getNumberOfShards(); shardId++) {
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(new ShardId(indexMetaData.getIndex(), shardId));
                for (int i = 0; i <= indexMetaData.getNumberOfReplicas(); i++) {
                    indexShardRoutingBuilder.addShard(ShardRouting.newUnassigned(index, shardId, null, i == 0, unassignedInfo));
                }
                shards.put(shardId, indexShardRoutingBuilder.build());
            }
            return this;
        }

        public Builder addReplica() {
            for (IntCursor cursor : shards.keys()) {
                int shardId = cursor.value;
                // version 0, will get updated when reroute will happen
                ShardRouting shard = ShardRouting.newUnassigned(index, shardId, null, false, new UnassignedInfo(UnassignedInfo.Reason.REPLICA_ADDED, null));
                shards.put(shardId,
                        new IndexShardRoutingTable.Builder(shards.get(shard.id())).addShard(shard).build()
                );
            }
            return this;
        }

        public Builder removeReplica() {
            for (IntCursor cursor : shards.keys()) {
                int shardId = cursor.value;
                IndexShardRoutingTable indexShard = shards.get(shardId);
                if (indexShard.replicaShards().isEmpty()) {
                    // nothing to do here!
                    return this;
                }
                // re-add all the current ones
                IndexShardRoutingTable.Builder builder = new IndexShardRoutingTable.Builder(indexShard.shardId());
                for (ShardRouting shardRouting : indexShard) {
                    builder.addShard(new ShardRouting(shardRouting));
                }
                // first check if there is one that is not assigned to a node, and remove it
                boolean removed = false;
                for (ShardRouting shardRouting : indexShard) {
                    if (!shardRouting.primary() && !shardRouting.assignedToNode()) {
                        builder.removeShard(shardRouting);
                        removed = true;
                        break;
                    }
                }
                if (!removed) {
                    for (ShardRouting shardRouting : indexShard) {
                        if (!shardRouting.primary()) {
                            builder.removeShard(shardRouting);
                            break;
                        }
                    }
                }
                shards.put(shardId, builder.build());
            }
            return this;
        }

        public Builder addIndexShard(IndexShardRoutingTable indexShard) {
            shards.put(indexShard.shardId().id(), indexShard);
            return this;
        }

        /**
         * Adds a new shard routing (makes a copy of it), with reference data used from the index shard routing table
         * if it needs to be created.
         */
        public Builder addShard(IndexShardRoutingTable refData, ShardRouting shard) {
            IndexShardRoutingTable indexShard = shards.get(shard.id());
            if (indexShard == null) {
                indexShard = new IndexShardRoutingTable.Builder(refData.shardId()).addShard(new ShardRouting(shard)).build();
            } else {
                indexShard = new IndexShardRoutingTable.Builder(indexShard).addShard(new ShardRouting(shard)).build();
            }
            shards.put(indexShard.shardId().id(), indexShard);
            return this;
        }

        public IndexRoutingTable build() throws RoutingValidationException {
            IndexRoutingTable indexRoutingTable = new IndexRoutingTable(index, shards.build());
            indexRoutingTable.validate();
            return indexRoutingTable;
        }
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder("-- index [" + index + "]\n");

        List<IndexShardRoutingTable> ordered = new ArrayList<>();
        for (IndexShardRoutingTable indexShard : this) {
            ordered.add(indexShard);
        }

        CollectionUtil.timSort(ordered, new Comparator<IndexShardRoutingTable>() {
            @Override
            public int compare(IndexShardRoutingTable o1, IndexShardRoutingTable o2) {
                int v = o1.shardId().index().name().compareTo(
                        o2.shardId().index().name());
                if (v == 0) {
                    v = Integer.compare(o1.shardId().id(),
                                        o2.shardId().id());
                }
                return v;
            }
        });

        for (IndexShardRoutingTable indexShard : ordered) {
            sb.append("----shard_id [").append(indexShard.shardId().index().name()).append("][").append(indexShard.shardId().id()).append("]\n");
            for (ShardRouting shard : indexShard) {
                sb.append("--------").append(shard.shortSummary()).append("\n");
            }
        }
        return sb.toString();
    }


}
