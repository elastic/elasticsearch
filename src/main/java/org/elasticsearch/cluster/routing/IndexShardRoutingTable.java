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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.collect.Lists.newArrayList;

/**
 * {@link IndexShardRoutingTable} encapsulates all instances of a single shard.
 * Each Elasticsearch index consists of multiple shards, each shard encapsulates
 * a disjoint set of the index data and each shard has one or more instances
 * referred to as replicas of a shard. Given that, this class encapsulates all
 * replicas (instances) for a single index shard.
 */
public class IndexShardRoutingTable implements Iterable<ShardRouting> {

    final ShardShuffler shuffler;
    final ShardId shardId;

    final ShardRouting primary;
    final ImmutableList<ShardRouting> primaryAsList;
    final ImmutableList<ShardRouting> replicas;
    final ImmutableList<ShardRouting> shards;
    final ImmutableList<ShardRouting> activeShards;
    final ImmutableList<ShardRouting> assignedShards;
    final boolean allShardsStarted;

    /**
     * The initializing list, including ones that are initializing on a target node because of relocation.
     * If we can come up with a better variable name, it would be nice...
     */
    final ImmutableList<ShardRouting> allInitializingShards;

    final boolean primaryAllocatedPostApi;

    IndexShardRoutingTable(ShardId shardId, ImmutableList<ShardRouting> shards, boolean primaryAllocatedPostApi) {
        this.shardId = shardId;
        this.shuffler = new RotationShardShuffler(ThreadLocalRandom.current().nextInt());
        this.shards = shards;
        this.primaryAllocatedPostApi = primaryAllocatedPostApi;

        ShardRouting primary = null;
        ImmutableList.Builder<ShardRouting> replicas = ImmutableList.builder();
        ImmutableList.Builder<ShardRouting> activeShards = ImmutableList.builder();
        ImmutableList.Builder<ShardRouting> assignedShards = ImmutableList.builder();
        ImmutableList.Builder<ShardRouting> allInitializingShards = ImmutableList.builder();
        boolean allShardsStarted = true;
        for (ShardRouting shard : shards) {
            if (shard.primary()) {
                primary = shard;
            } else {
                replicas.add(shard);
            }
            if (shard.active()) {
                activeShards.add(shard);
            }
            if (shard.initializing()) {
                allInitializingShards.add(shard);
            }
            if (shard.relocating()) {
                // create the target initializing shard routing on the node the shard is relocating to
                allInitializingShards.add(shard.targetRoutingIfRelocating());
            }
            if (shard.assignedToNode()) {
                assignedShards.add(shard);
            }
            if (shard.state() != ShardRoutingState.STARTED) {
                allShardsStarted = false;
            }
        }
        this.allShardsStarted = allShardsStarted;

        this.primary = primary;
        if (primary != null) {
            this.primaryAsList = ImmutableList.of(primary);
        } else {
            this.primaryAsList = ImmutableList.of();
        }
        this.replicas = replicas.build();
        this.activeShards = activeShards.build();
        this.assignedShards = assignedShards.build();
        this.allInitializingShards = allInitializingShards.build();
    }

    /**
     * Normalizes all shard routings to the same version.
     */
    public IndexShardRoutingTable normalizeVersions() {
        if (shards.isEmpty()) {
            return this;
        }
        if (shards.size() == 1) {
            return this;
        }
        long highestVersion = shards.get(0).version();
        boolean requiresNormalization = false;
        for (int i = 1; i < shards.size(); i++) {
            if (shards.get(i).version() != highestVersion) {
                requiresNormalization = true;
            }
            if (shards.get(i).version() > highestVersion) {
                highestVersion = shards.get(i).version();
            }
        }
        if (!requiresNormalization) {
            return this;
        }
        List<ShardRouting> shardRoutings = new ArrayList<>(shards.size());
        for (int i = 0; i < shards.size(); i++) {
            if (shards.get(i).version() == highestVersion) {
                shardRoutings.add(shards.get(i));
            } else {
                shardRoutings.add(new ImmutableShardRouting(shards.get(i), highestVersion));
            }
        }
        return new IndexShardRoutingTable(shardId, ImmutableList.copyOf(shardRoutings), primaryAllocatedPostApi);
    }

    /**
     * Has this shard group primary shard been allocated post API creation. Will be set to
     * <code>true</code> if it was created because of recovery action.
     */
    public boolean primaryAllocatedPostApi() {
        return primaryAllocatedPostApi;
    }

    /**
     * Returns the shards id
     *
     * @return id of the shard
     */
    public ShardId shardId() {
        return shardId;
    }

    /**
     * Returns the shards id
     *
     * @return id of the shard
     */
    public ShardId getShardId() {
        return shardId();
    }

    @Override
    public UnmodifiableIterator<ShardRouting> iterator() {
        return shards.iterator();
    }

    /**
     * Returns the number of this shards instances.
     */
    public int size() {
        return shards.size();
    }

    /**
     * Returns the number of this shards instances.
     */
    public int getSize() {
        return size();
    }

    /**
     * Returns a {@link ImmutableList} of shards
     *
     * @return a {@link ImmutableList} of shards
     */
    public ImmutableList<ShardRouting> shards() {
        return this.shards;
    }

    /**
     * Returns a {@link ImmutableList} of shards
     *
     * @return a {@link ImmutableList} of shards
     */
    public ImmutableList<ShardRouting> getShards() {
        return shards();
    }

    /**
     * Returns a {@link ImmutableList} of active shards
     *
     * @return a {@link ImmutableList} of shards
     */
    public ImmutableList<ShardRouting> activeShards() {
        return this.activeShards;
    }

    /**
     * Returns a {@link ImmutableList} of active shards
     *
     * @return a {@link ImmutableList} of shards
     */
    public ImmutableList<ShardRouting> getActiveShards() {
        return activeShards();
    }

    /**
     * Returns a {@link ImmutableList} of assigned shards
     *
     * @return a {@link ImmutableList} of shards
     */
    public ImmutableList<ShardRouting> assignedShards() {
        return this.assignedShards;
    }

    /**
     * Returns a {@link ImmutableList} of assigned shards
     *
     * @return a {@link ImmutableList} of shards
     */
    public ImmutableList<ShardRouting> getAssignedShards() {
        return this.assignedShards;
    }

    public ShardIterator shardsRandomIt() {
        return new PlainShardIterator(shardId, shuffler.shuffle(shards));
    }

    public ShardIterator shardsIt() {
        return new PlainShardIterator(shardId, shards);
    }

    public ShardIterator shardsIt(int seed) {
        return new PlainShardIterator(shardId, shuffler.shuffle(shards, seed));
    }

    /**
     * Returns an iterator over active and initializing shards. Making sure though that
     * its random within the active shards, and initializing shards are the last to iterate through.
     */
    public ShardIterator activeInitializingShardsRandomIt() {
        return activeInitializingShardsIt(shuffler.nextSeed());
    }

    /**
     * Returns an iterator over active and initializing shards. Making sure though that
     * its random within the active shards, and initializing shards are the last to iterate through.
     */
    public ShardIterator activeInitializingShardsIt(int seed) {
        if (allInitializingShards.isEmpty()) {
            return new PlainShardIterator(shardId, shuffler.shuffle(activeShards, seed));
        }
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        ordered.addAll(shuffler.shuffle(activeShards, seed));
        ordered.addAll(allInitializingShards);
        return new PlainShardIterator(shardId, ordered);
    }

    /**
     * Returns an iterator only on the primary shard.
     */
    public ShardIterator primaryShardIt() {
        return new PlainShardIterator(shardId, primaryAsList);
    }

    public ShardIterator primaryActiveInitializingShardIt() {
        if (!primaryAsList.isEmpty() && !primaryAsList.get(0).active() && !primaryAsList.get(0).initializing()) {
            List<ShardRouting> primaryList = ImmutableList.of();
            return new PlainShardIterator(shardId, primaryList);
        }
        return primaryShardIt();
    }

    public ShardIterator primaryFirstActiveInitializingShardsIt() {
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        // fill it in a randomized fashion
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards)) {
            ordered.add(shardRouting);
            if (shardRouting.primary()) {
                // switch, its the matching node id
                ordered.set(ordered.size() - 1, ordered.get(0));
                ordered.set(0, shardRouting);
            }
        }
        // no need to worry about primary first here..., its temporal
        if (!allInitializingShards.isEmpty()) {
            ordered.addAll(allInitializingShards);
        }
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardIterator onlyNodeActiveInitializingShardsIt(String nodeId) {
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        // fill it in a randomized fashion
        for (int i = 0; i < activeShards.size(); i++) {
            ShardRouting shardRouting = activeShards.get(i);
            if (nodeId.equals(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        for (int i = 0; i < allInitializingShards.size(); i++) {
            ShardRouting shardRouting = allInitializingShards.get(i);
            if (nodeId.equals(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardIterator preferNodeActiveInitializingShardsIt(String nodeId) {
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        // fill it in a randomized fashion
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards)) {
            ordered.add(shardRouting);
            if (nodeId.equals(shardRouting.currentNodeId())) {
                // switch, its the matching node id
                ordered.set(ordered.size() - 1, ordered.get(0));
                ordered.set(0, shardRouting);
            }
        }
        if (!allInitializingShards.isEmpty()) {
            ordered.addAll(allInitializingShards);
        }
        return new PlainShardIterator(shardId, ordered);
    }

    /**
     * Returns <code>true</code> iff all shards in the routing table are started otherwise <code>false</code>
     */
    public boolean allShardsStarted() {
        return allShardsStarted;
    }

    static class AttributesKey {

        final String[] attributes;

        AttributesKey(String[] attributes) {
            this.attributes = attributes;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(attributes);
        }

        @Override
        public boolean equals(Object obj) {
            return Arrays.equals(attributes, ((AttributesKey) obj).attributes);
        }
    }

    static class AttributesRoutings {

        public final ImmutableList<ShardRouting> withSameAttribute;
        public final ImmutableList<ShardRouting> withoutSameAttribute;
        public final int totalSize;

        AttributesRoutings(ImmutableList<ShardRouting> withSameAttribute, ImmutableList<ShardRouting> withoutSameAttribute) {
            this.withSameAttribute = withSameAttribute;
            this.withoutSameAttribute = withoutSameAttribute;
            this.totalSize = withoutSameAttribute.size() + withSameAttribute.size();
        }
    }

    private volatile Map<AttributesKey, AttributesRoutings> activeShardsByAttributes = ImmutableMap.of();
    private volatile Map<AttributesKey, AttributesRoutings> initializingShardsByAttributes = ImmutableMap.of();
    private final Object shardsByAttributeMutex = new Object();

    private AttributesRoutings getActiveAttribute(AttributesKey key, DiscoveryNodes nodes) {
        AttributesRoutings shardRoutings = activeShardsByAttributes.get(key);
        if (shardRoutings == null) {
            synchronized (shardsByAttributeMutex) {
                ArrayList<ShardRouting> from = new ArrayList<>(activeShards);
                ImmutableList<ShardRouting> to = collectAttributeShards(key, nodes, from);

                shardRoutings = new AttributesRoutings(to, ImmutableList.copyOf(from));
                activeShardsByAttributes = MapBuilder.newMapBuilder(activeShardsByAttributes).put(key, shardRoutings).immutableMap();
            }
        }
        return shardRoutings;
    }

    private AttributesRoutings getInitializingAttribute(AttributesKey key, DiscoveryNodes nodes) {
        AttributesRoutings shardRoutings = initializingShardsByAttributes.get(key);
        if (shardRoutings == null) {
            synchronized (shardsByAttributeMutex) {
                ArrayList<ShardRouting> from = new ArrayList<>(allInitializingShards);
                ImmutableList<ShardRouting> to = collectAttributeShards(key, nodes, from);
                shardRoutings = new AttributesRoutings(to, ImmutableList.copyOf(from));
                initializingShardsByAttributes = MapBuilder.newMapBuilder(initializingShardsByAttributes).put(key, shardRoutings).immutableMap();
            }
        }
        return shardRoutings;
    }

    private static ImmutableList<ShardRouting> collectAttributeShards(AttributesKey key, DiscoveryNodes nodes, ArrayList<ShardRouting> from) {
        final ArrayList<ShardRouting> to = new ArrayList<>();
        for (final String attribute : key.attributes) {
            final String localAttributeValue = nodes.localNode().attributes().get(attribute);
            if (localAttributeValue != null) {
                for (Iterator<ShardRouting> iterator = from.iterator(); iterator.hasNext(); ) {
                    ShardRouting fromShard = iterator.next();
                    final DiscoveryNode discoveryNode = nodes.get(fromShard.currentNodeId());
                    if (discoveryNode == null) {
                        iterator.remove(); // node is not present anymore - ignore shard
                    } else if (localAttributeValue.equals(discoveryNode.attributes().get(attribute))) {
                        iterator.remove();
                        to.add(fromShard);
                    }
                }
            }
        }
        return ImmutableList.copyOf(to);
    }

    public ShardIterator preferAttributesActiveInitializingShardsIt(String[] attributes, DiscoveryNodes nodes) {
        return preferAttributesActiveInitializingShardsIt(attributes, nodes, shuffler.nextSeed());
    }

    public ShardIterator preferAttributesActiveInitializingShardsIt(String[] attributes, DiscoveryNodes nodes, int seed) {
        AttributesKey key = new AttributesKey(attributes);
        AttributesRoutings activeRoutings = getActiveAttribute(key, nodes);
        AttributesRoutings initializingRoutings = getInitializingAttribute(key, nodes);

        // we now randomize, once between the ones that have the same attributes, and once for the ones that don't
        // we don't want to mix between the two!
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeRoutings.totalSize + initializingRoutings.totalSize);
        ordered.addAll(shuffler.shuffle(activeRoutings.withSameAttribute, seed));
        ordered.addAll(shuffler.shuffle(activeRoutings.withoutSameAttribute, seed));
        ordered.addAll(shuffler.shuffle(initializingRoutings.withSameAttribute, seed));
        ordered.addAll(shuffler.shuffle(initializingRoutings.withoutSameAttribute, seed));
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardRouting primaryShard() {
        return primary;
    }

    public List<ShardRouting> replicaShards() {
        return this.replicas;
    }

    public List<ShardRouting> replicaShardsWithState(ShardRoutingState... states) {
        List<ShardRouting> shards = newArrayList();
        for (ShardRouting shardEntry : replicas) {
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    shards.add(shardEntry);
                }
            }
        }
        return shards;
    }

    public List<ShardRouting> shardsWithState(ShardRoutingState state) {
        if (state == ShardRoutingState.INITIALIZING) {
            return allInitializingShards;
        }
        List<ShardRouting> shards = newArrayList();
        for (ShardRouting shardEntry : this) {
            if (shardEntry.state() == state) {
                shards.add(shardEntry);
            }
        }
        return shards;
    }

    public static class Builder {

        private ShardId shardId;

        private final List<ShardRouting> shards;

        private boolean primaryAllocatedPostApi;

        public Builder(IndexShardRoutingTable indexShard) {
            this.shardId = indexShard.shardId;
            this.shards = newArrayList(indexShard.shards);
            this.primaryAllocatedPostApi = indexShard.primaryAllocatedPostApi();
        }

        public Builder(ShardId shardId, boolean primaryAllocatedPostApi) {
            this.shardId = shardId;
            this.shards = newArrayList();
            this.primaryAllocatedPostApi = primaryAllocatedPostApi;
        }

        public Builder addShard(ImmutableShardRouting shardEntry) {
            for (ShardRouting shard : shards) {
                // don't add two that map to the same node id
                // we rely on the fact that a node does not have primary and backup of the same shard
                if (shard.assignedToNode() && shardEntry.assignedToNode()
                        && shard.currentNodeId().equals(shardEntry.currentNodeId())) {
                    return this;
                }
            }
            shards.add(shardEntry);
            return this;
        }

        public Builder removeShard(ShardRouting shardEntry) {
            shards.remove(shardEntry);
            return this;
        }

        public IndexShardRoutingTable build() {
            // we can automatically set allocatedPostApi to true if the primary is active
            if (!primaryAllocatedPostApi) {
                for (ShardRouting shardRouting : shards) {
                    if (shardRouting.primary() && shardRouting.active()) {
                        primaryAllocatedPostApi = true;
                    }
                }
            }
            return new IndexShardRoutingTable(shardId, ImmutableList.copyOf(shards), primaryAllocatedPostApi);
        }

        public static IndexShardRoutingTable readFrom(StreamInput in) throws IOException {
            String index = in.readString();
            return readFromThin(in, index);
        }

        public static IndexShardRoutingTable readFromThin(StreamInput in, String index) throws IOException {
            int iShardId = in.readVInt();
            boolean allocatedPostApi = in.readBoolean();
            Builder builder = new Builder(new ShardId(index, iShardId), allocatedPostApi);

            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                ImmutableShardRouting shard = ImmutableShardRouting.readShardRoutingEntry(in, index, iShardId);
                builder.addShard(shard);
            }

            return builder.build();
        }

        public static void writeTo(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            out.writeString(indexShard.shardId().index().name());
            writeToThin(indexShard, out);
        }

        public static void writeToThin(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            out.writeVInt(indexShard.shardId.id());
            out.writeBoolean(indexShard.primaryAllocatedPostApi());

            out.writeVInt(indexShard.shards.size());
            for (ShardRouting entry : indexShard) {
                entry.writeToThin(out);
            }
        }

    }
}
