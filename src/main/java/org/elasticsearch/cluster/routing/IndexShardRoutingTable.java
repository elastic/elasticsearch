/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this 
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import jsr166y.ThreadLocalRandom;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.newArrayList;

/**
 * {@link IndexShardRoutingTable} encapsulates all instances or a single shard.
 * Each ElasticSearch index consists of multiple shards each shards encapsulates
 * a disjoint set of the index data and earch shard has one more more instances
 * refered to as replicas of a shard. Given that, this class encapsulates all
 * replicas (instances) for a single index shard.
 */
public class IndexShardRoutingTable implements Iterable<ShardRouting> {

    final ShardId shardId;

    final ShardRouting primary;
    final ImmutableList<ShardRouting> primaryAsList;
    final ImmutableList<ShardRouting> replicas;
    final ImmutableList<ShardRouting> shards;
    final ImmutableList<ShardRouting> activeShards;
    final ImmutableList<ShardRouting> assignedShards;

    final AtomicInteger counter;

    final boolean primaryAllocatedPostApi;

    IndexShardRoutingTable(ShardId shardId, ImmutableList<ShardRouting> shards, boolean primaryAllocatedPostApi) {
        this.shardId = shardId;
        this.shards = shards;
        this.primaryAllocatedPostApi = primaryAllocatedPostApi;
        this.counter = new AtomicInteger(ThreadLocalRandom.current().nextInt(shards.size()));

        ShardRouting primary = null;
        ImmutableList.Builder<ShardRouting> replicas = ImmutableList.builder();
        ImmutableList.Builder<ShardRouting> activeShards = ImmutableList.builder();
        ImmutableList.Builder<ShardRouting> assignedShards = ImmutableList.builder();

        for (ShardRouting shard : shards) {
            if (shard.primary()) {
                primary = shard;
            } else {
                replicas.add(shard);
            }
            if (shard.active()) {
                activeShards.add(shard);
            }
            if (shard.assignedToNode()) {
                assignedShards.add(shard);
            }
        }

        this.primary = primary;
        if (primary != null) {
            this.primaryAsList = ImmutableList.of(primary);
        } else {
            this.primaryAsList = ImmutableList.of();
        }
        this.replicas = replicas.build();
        this.activeShards = activeShards.build();
        this.assignedShards = assignedShards.build();
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
        List<ShardRouting> shardRoutings = new ArrayList<ShardRouting>(shards.size());
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

    /**
     * Returns the number of shards in a specific state
     *
     * @param state state of the shards to count
     * @return number of shards in <code>state</code>
     */
    public int countWithState(ShardRoutingState state) {
        int count = 0;
        for (ShardRouting shard : this) {
            if (state == shard.state()) {
                count++;
            }
        }
        return count;
    }

    public ShardIterator shardsRandomIt() {
        return new PlainShardIterator(shardId, shards, counter.getAndIncrement());
    }

    public ShardIterator shardsIt() {
        return new PlainShardIterator(shardId, shards);
    }

    public ShardIterator shardsIt(int index) {
        return new PlainShardIterator(shardId, shards, index);
    }

    public ShardIterator activeShardsRandomIt() {
        return new PlainShardIterator(shardId, activeShards, counter.getAndIncrement());
    }

    public ShardIterator activeShardsIt() {
        return new PlainShardIterator(shardId, activeShards);
    }

    public ShardIterator activeShardsIt(int index) {
        return new PlainShardIterator(shardId, activeShards, index);
    }

    public ShardIterator assignedShardsRandomIt() {
        return new PlainShardIterator(shardId, assignedShards, counter.getAndIncrement());
    }

    public ShardIterator assignedShardsIt() {
        return new PlainShardIterator(shardId, assignedShards);
    }

    public ShardIterator assignedShardsIt(int index) {
        return new PlainShardIterator(shardId, assignedShards, index);
    }

    /**
     * Returns an iterator only on the primary shard.
     */
    public ShardIterator primaryShardIt() {
        return new PlainShardIterator(shardId, primaryAsList);
    }

    public ShardIterator primaryFirstActiveShardsIt() {
        ArrayList<ShardRouting> ordered = new ArrayList<ShardRouting>(activeShards.size());
        // fill it in a randomized fashion
        int index = Math.abs(counter.getAndIncrement());
        for (int i = 0; i < activeShards.size(); i++) {
            int loc = (index + i) % activeShards.size();
            ShardRouting shardRouting = activeShards.get(loc);
            ordered.add(shardRouting);
            if (shardRouting.primary()) {
                // switch, its the matching node id
                ordered.set(i, ordered.get(0));
                ordered.set(0, shardRouting);
            }
        }
        return new PlainShardIterator(shardId, ordered);
    }

    /**
     * Prefers execution on the provided node if applicable.
     */
    public ShardIterator preferNodeShardsIt(String nodeId) {
        return preferNodeShardsIt(nodeId, shards);
    }

    public ShardIterator onlyNodeActiveShardsIt(String nodeId) {
        ArrayList<ShardRouting> ordered = new ArrayList<ShardRouting>(shards.size());
        // fill it in a randomized fashion
        for (int i = 0; i < shards.size(); i++) {
            ShardRouting shardRouting = shards.get(i);
            if (nodeId.equals(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        return new PlainShardIterator(shardId, ordered);
    }

    /**
     * Prefers execution on the provided node if applicable.
     */
    public ShardIterator preferNodeActiveShardsIt(String nodeId) {
        return preferNodeShardsIt(nodeId, activeShards);
    }

    /**
     * Prefers execution on the provided node if applicable.
     */
    public ShardIterator preferNodeAssignedShardsIt(String nodeId) {
        return preferNodeShardsIt(nodeId, assignedShards);
    }

    private ShardIterator preferNodeShardsIt(String nodeId, ImmutableList<ShardRouting> shards) {
        ArrayList<ShardRouting> ordered = new ArrayList<ShardRouting>(shards.size());
        // fill it in a randomized fashion
        int index = Math.abs(counter.getAndIncrement());
        for (int i = 0; i < shards.size(); i++) {
            int loc = (index + i) % shards.size();
            ShardRouting shardRouting = shards.get(loc);
            ordered.add(shardRouting);
            if (nodeId.equals(shardRouting.currentNodeId())) {
                // switch, its the matching node id
                ordered.set(i, ordered.get(0));
                ordered.set(0, shardRouting);
            }
        }
        return new PlainShardIterator(shardId, ordered);
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
    private final Object shardsByAttributeMutex = new Object();

    public ShardIterator preferAttributesActiveShardsIt(String[] attributes, DiscoveryNodes nodes) {
        return preferAttributesActiveShardsIt(attributes, nodes, counter.incrementAndGet());
    }

    public ShardIterator preferAttributesActiveShardsIt(String[] attributes, DiscoveryNodes nodes, int index) {
        AttributesKey key = new AttributesKey(attributes);
        AttributesRoutings shardRoutings = activeShardsByAttributes.get(key);
        if (shardRoutings == null) {
            synchronized (shardsByAttributeMutex) {
                ArrayList<ShardRouting> from = new ArrayList<ShardRouting>(activeShards);
                ArrayList<ShardRouting> to = new ArrayList<ShardRouting>();
                for (String attribute : attributes) {
                    String localAttributeValue = nodes.localNode().attributes().get(attribute);
                    if (localAttributeValue == null) {
                        continue;
                    }
                    for (Iterator<ShardRouting> iterator = from.iterator(); iterator.hasNext(); ) {
                        ShardRouting fromShard = iterator.next();
                        if (localAttributeValue.equals(nodes.get(fromShard.currentNodeId()).attributes().get(attribute))) {
                            iterator.remove();
                            to.add(fromShard);
                        }
                    }
                }

                shardRoutings = new AttributesRoutings(ImmutableList.copyOf(to), ImmutableList.copyOf(from));
                activeShardsByAttributes = MapBuilder.newMapBuilder(activeShardsByAttributes).put(key, shardRoutings).immutableMap();
            }
        }
        // we now randomize, once between the ones that have the same attributes, and once for the ones that don't
        // we don't want to mix between the two!
        ArrayList<ShardRouting> ordered = new ArrayList<ShardRouting>(shardRoutings.totalSize);
        index = Math.abs(index);
        for (int i = 0; i < shardRoutings.withSameAttribute.size(); i++) {
            int loc = (index + i) % shardRoutings.withSameAttribute.size();
            ShardRouting shardRouting = shardRoutings.withSameAttribute.get(loc);
            ordered.add(shardRouting);
        }
        for (int i = 0; i < shardRoutings.withoutSameAttribute.size(); i++) {
            int loc = (index + i) % shardRoutings.withoutSameAttribute.size();
            ShardRouting shardRouting = shardRoutings.withoutSameAttribute.get(loc);
            ordered.add(shardRouting);
        }

        return new PlainShardIterator(shardId, ordered);
    }

    public ShardRouting primaryShard() {
        return primary;
    }

    public List<ShardRouting> replicaShards() {
        return this.replicas;
    }

    public List<ShardRouting> shardsWithState(ShardRoutingState... states) {
        List<ShardRouting> shards = newArrayList();
        for (ShardRouting shardEntry : this) {
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    shards.add(shardEntry);
                }
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
