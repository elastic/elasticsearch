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

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.shard.ShardId;

import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

/**
 * {@link RoutingNodes} represents a copy the routing information contained in
 * the {@link ClusterState cluster state}.
 */
public class RoutingNodes implements Iterable<RoutingNode> {

    private final MetaData metaData;

    private final ClusterBlocks blocks;

    private final RoutingTable routingTable;

    private final Map<String, RoutingNode> nodesToShards = newHashMap();

    private final UnassignedShards unassignedShards = new UnassignedShards();

    private final List<ShardRouting> ignoredUnassignedShards = newArrayList();

    private final Map<ShardId, List<ShardRouting>> assignedShards = newHashMap();

    private final ImmutableOpenMap<String, ClusterState.Custom> customs;

    private int inactivePrimaryCount = 0;

    private int inactiveShardCount = 0;

    private int relocatingShards = 0;

    private Set<ShardId> clearPostAllocationFlag;

    private final Map<String, ObjectIntHashMap<String>> nodesPerAttributeNames = new HashMap<>();

    public RoutingNodes(ClusterState clusterState) {
        this.metaData = clusterState.metaData();
        this.blocks = clusterState.blocks();
        this.routingTable = clusterState.routingTable();
        this.customs = clusterState.customs();

        Map<String, List<ShardRouting>> nodesToShards = newHashMap();
        // fill in the nodeToShards with the "live" nodes
        for (ObjectCursor<DiscoveryNode> cursor : clusterState.nodes().dataNodes().values()) {
            nodesToShards.put(cursor.value.id(), new ArrayList<ShardRouting>());
        }

        // fill in the inverse of node -> shards allocated
        // also fill replicaSet information
        for (IndexRoutingTable indexRoutingTable : routingTable.indicesRouting().values()) {
            for (IndexShardRoutingTable indexShard : indexRoutingTable) {
                for (ShardRouting shard : indexShard) {
                    // to get all the shards belonging to an index, including the replicas,
                    // we define a replica set and keep track of it. A replica set is identified
                    // by the ShardId, as this is common for primary and replicas.
                    // A replica Set might have one (and not more) replicas with the state of RELOCATING.
                    if (shard.assignedToNode()) {
                        List<ShardRouting> entries = nodesToShards.get(shard.currentNodeId());
                        if (entries == null) {
                            entries = newArrayList();
                            nodesToShards.put(shard.currentNodeId(), entries);
                        }
                        ShardRouting sr = new ShardRouting(shard);
                        entries.add(sr);
                        assignedShardsAdd(sr);
                        if (shard.relocating()) {
                            entries = nodesToShards.get(shard.relocatingNodeId());
                            relocatingShards++;
                            if (entries == null) {
                                entries = newArrayList();
                                nodesToShards.put(shard.relocatingNodeId(), entries);
                            }
                            // add the counterpart shard with relocatingNodeId reflecting the source from which
                            // it's relocating from.
                            sr = shard.buildTargetRelocatingShard();
                            entries.add(sr);
                            assignedShardsAdd(sr);
                        } else if (!shard.active()) { // shards that are initializing without being relocated
                            if (shard.primary()) {
                                inactivePrimaryCount++;
                            }
                            inactiveShardCount++;
                        }
                    } else {
                        ShardRouting sr = new ShardRouting(shard);
                        assignedShardsAdd(sr);
                        unassignedShards.add(sr);
                    }
                }
            }
        }
        for (Map.Entry<String, List<ShardRouting>> entry : nodesToShards.entrySet()) {
            String nodeId = entry.getKey();
            this.nodesToShards.put(nodeId, new RoutingNode(nodeId, clusterState.nodes().get(nodeId), entry.getValue()));
        }
    }

    @Override
    public Iterator<RoutingNode> iterator() {
        return Iterators.unmodifiableIterator(nodesToShards.values().iterator());
    }

    public RoutingTable routingTable() {
        return routingTable;
    }

    public RoutingTable getRoutingTable() {
        return routingTable();
    }

    public MetaData metaData() {
        return this.metaData;
    }

    public MetaData getMetaData() {
        return metaData();
    }

    public ClusterBlocks blocks() {
        return this.blocks;
    }

    public ClusterBlocks getBlocks() {
        return this.blocks;
    }

    public ImmutableOpenMap<String, ClusterState.Custom> customs() {
        return this.customs;
    }

    public <T extends ClusterState.Custom> T custom(String type) {
        return (T) customs.get(type);
    }

    public int requiredAverageNumberOfShardsPerNode() {
        int totalNumberOfShards = 0;
        // we need to recompute to take closed shards into account
        for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
            IndexMetaData indexMetaData = cursor.value;
            if (indexMetaData.state() == IndexMetaData.State.OPEN) {
                totalNumberOfShards += indexMetaData.totalNumberOfShards();
            }
        }
        return totalNumberOfShards / nodesToShards.size();
    }

    public boolean hasUnassigned() {
        return !unassignedShards.isEmpty();
    }

    public List<ShardRouting> ignoredUnassigned() {
        return this.ignoredUnassignedShards;
    }

    public UnassignedShards unassigned() {
        return this.unassignedShards;
    }

    public RoutingNodesIterator nodes() {
        return new RoutingNodesIterator(nodesToShards.values().iterator());
    }

    /**
     * Clears the post allocation flag for the provided shard id. NOTE: this should be used cautiously
     * since it will lead to data loss of the primary shard is not allocated, as it will allocate
     * the primary shard on a node and *not* expect it to have an existing valid index there.
     */
    public void addClearPostAllocationFlag(ShardId shardId) {
        if (clearPostAllocationFlag == null) {
            clearPostAllocationFlag = Sets.newHashSet();
        }
        clearPostAllocationFlag.add(shardId);
    }

    public Iterable<ShardId> getShardsToClearPostAllocationFlag() {
        if (clearPostAllocationFlag == null) {
            return ImmutableSet.of();
        }
        return clearPostAllocationFlag;
    }

    public RoutingNode node(String nodeId) {
        return nodesToShards.get(nodeId);
    }

    public ObjectIntHashMap<String> nodesPerAttributesCounts(String attributeName) {
        ObjectIntHashMap<String> nodesPerAttributesCounts = nodesPerAttributeNames.get(attributeName);
        if (nodesPerAttributesCounts != null) {
            return nodesPerAttributesCounts;
        }
        nodesPerAttributesCounts = new ObjectIntHashMap<>();
        for (RoutingNode routingNode : this) {
            String attrValue = routingNode.node().attributes().get(attributeName);
            nodesPerAttributesCounts.addTo(attrValue, 1);
        }
        nodesPerAttributeNames.put(attributeName, nodesPerAttributesCounts);
        return nodesPerAttributesCounts;
    }

    public boolean hasUnassignedPrimaries() {
        return unassignedShards.numPrimaries() > 0;
    }

    public boolean hasUnassignedShards() {
        return !unassignedShards.isEmpty();
    }

    public boolean hasInactivePrimaries() {
        return inactivePrimaryCount > 0;
    }

    public boolean hasInactiveShards() {
        return inactiveShardCount > 0;
    }

    public int getRelocatingShardCount() {
        return relocatingShards;
    }

    /**
     * Returns the active primary shard for the given ShardRouting or <code>null</code> if
     * no primary is found or the primary is not active.
     */
    public ShardRouting activePrimary(ShardRouting shard) {
        for (ShardRouting shardRouting : assignedShards(shard.shardId())) {
            if (shardRouting.primary() && shardRouting.active()) {
                return shardRouting;
            }
        }
        return null;
    }

    /**
     * Returns one active replica shard for the given ShardRouting shard ID or <code>null</code> if
     * no active replica is found.
     */
    public ShardRouting activeReplica(ShardRouting shard) {
        for (ShardRouting shardRouting : assignedShards(shard.shardId())) {
            if (!shardRouting.primary() && shardRouting.active()) {
                return shardRouting;
            }
        }
        return null;
    }

    /**
     * Returns all shards that are not in the state UNASSIGNED with the same shard
     * ID as the given shard.
     */
    public Iterable<ShardRouting> assignedShards(ShardRouting shard) {
        return assignedShards(shard.shardId());
    }

    /**
     * Returns <code>true</code> iff all replicas are active for the given shard routing. Otherwise <code>false</code>
     */
    public boolean allReplicasActive(ShardRouting shardRouting) {
        final List<ShardRouting> shards = assignedShards(shardRouting.shardId());
        if (shards.isEmpty() || shards.size() < this.routingTable.index(shardRouting.index()).shard(shardRouting.id()).size()) {
            return false; // if we are empty nothing is active if we have less than total at least one is unassigned
        }
        for (ShardRouting shard : shards) {
            if (!shard.active()) {
                return false;
            }
        }
        return true;
    }

    public List<ShardRouting> shards(Predicate<ShardRouting> predicate) {
        List<ShardRouting> shards = newArrayList();
        for (RoutingNode routingNode : this) {
            for (ShardRouting shardRouting : routingNode) {
                if (predicate.apply(shardRouting)) {
                    shards.add(shardRouting);
                }
            }
        }
        return shards;
    }

    public List<ShardRouting> shardsWithState(ShardRoutingState... state) {
        // TODO these are used on tests only - move into utils class
        List<ShardRouting> shards = newArrayList();
        for (RoutingNode routingNode : this) {
            shards.addAll(routingNode.shardsWithState(state));
        }
        for (ShardRoutingState s : state) {
            if (s == ShardRoutingState.UNASSIGNED) {
                Iterables.addAll(shards, unassigned());
                break;
            }
        }
        return shards;
    }

    public List<ShardRouting> shardsWithState(String index, ShardRoutingState... state) {
        // TODO these are used on tests only - move into utils class
        List<ShardRouting> shards = newArrayList();
        for (RoutingNode routingNode : this) {
            shards.addAll(routingNode.shardsWithState(index, state));
        }
        for (ShardRoutingState s : state) {
            if (s == ShardRoutingState.UNASSIGNED) {
                for (ShardRouting unassignedShard : unassignedShards) {
                    if (unassignedShard.index().equals(index)) {
                        shards.add(unassignedShard);
                    }
                }
                break;
            }
        }
        return shards;
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder("routing_nodes:\n");
        for (RoutingNode routingNode : this) {
            sb.append(routingNode.prettyPrint());
        }
        sb.append("---- unassigned\n");
        for (ShardRouting shardEntry : unassignedShards) {
            sb.append("--------").append(shardEntry.shortSummary()).append('\n');
        }
        return sb.toString();
    }

    /**
     * Assign a shard to a node. This will increment the inactiveShardCount counter
     * and the inactivePrimaryCount counter if the shard is the primary.
     * In case the shard is already assigned and started, it will be marked as 
     * relocating, which is accounted for, too, so the number of concurrent relocations
     * can be retrieved easily.
     * This method can be called several times for the same shard, only the first time
     * will change the state.
     *
     * INITIALIZING => INITIALIZING
     * UNASSIGNED   => INITIALIZING
     * STARTED      => RELOCATING
     * RELOCATING   => RELOCATING
     *
     * @param shard the shard to be assigned
     * @param nodeId the nodeId this shard should initialize on or relocate from
     */
    public void assign(ShardRouting shard, String nodeId) {
        // state will not change if the shard is already initializing.
        ShardRoutingState oldState = shard.state();
        shard.assignToNode(nodeId);
        node(nodeId).add(shard);
        if (oldState == ShardRoutingState.UNASSIGNED) {
            inactiveShardCount++;
            if (shard.primary()) {
                inactivePrimaryCount++;
            }
        }

        if (shard.state() == ShardRoutingState.RELOCATING) {
            relocatingShards++;
        }
        assignedShardsAdd(shard);
    }

    /**
     * Relocate a shard to another node, adding the target initializing
     * shard as well as assigning it. And returning the target initializing
     * shard.
     */
    public ShardRouting relocate(ShardRouting shard, String nodeId) {
        relocatingShards++;
        shard.relocate(nodeId);
        ShardRouting target = shard.buildTargetRelocatingShard();
        assign(target, target.currentNodeId());
        return target;
    }

    /**
     * Mark a shard as started and adjusts internal statistics.
     */
    public void started(ShardRouting shard) {
        if (!shard.active() && shard.relocatingNodeId() == null) {
            inactiveShardCount--;
            if (shard.primary()) {
                inactivePrimaryCount--;
            }
        } else if (shard.relocating()) {
            relocatingShards--;
        }
        assert !shard.started();
        shard.moveToStarted();
    }

    /**
     * Cancels a relocation of a shard that shard must relocating.
     */
    public void cancelRelocation(ShardRouting shard) {
        relocatingShards--;
        shard.cancelRelocation();
    }

    /**
     * swaps the status of a shard, making replicas primary and vice versa.
     *
     * @param shards the shard to have its primary status swapped.
     */
    public void swapPrimaryFlag(ShardRouting... shards) {
        for (ShardRouting shard : shards) {
            if (shard.primary()) {
                shard.moveFromPrimary();
                if (shard.unassigned()) {
                    unassignedShards.primaries--;
                }
            } else {
                shard.moveToPrimary();
                if (shard.unassigned()) {
                    unassignedShards.primaries++;
                }
            }
        }
    }

    private static final List<ShardRouting> EMPTY = Collections.emptyList();

    private List<ShardRouting> assignedShards(ShardId shardId) {
        final List<ShardRouting> replicaSet = assignedShards.get(shardId);
        return replicaSet == null ? EMPTY : Collections.unmodifiableList(replicaSet);
    }

    /**
     * Cancels the give shard from the Routing nodes internal statistics and cancels
     * the relocation if the shard is relocating.
     * @param shard
     */
    private void remove(ShardRouting shard) {
        if (!shard.active() && shard.relocatingNodeId() == null) {
            inactiveShardCount--;
            assert inactiveShardCount >= 0;
            if (shard.primary()) {
                inactivePrimaryCount--;
            }
        } else if (shard.relocating()) {
            cancelRelocation(shard);
        }
        assignedShardsRemove(shard);
    }

    private void assignedShardsAdd(ShardRouting shard) {
        if (shard.unassigned()) {
            // no unassigned
            return;
        }
        List<ShardRouting> shards = assignedShards.get(shard.shardId());
        if (shards == null) {
            shards = Lists.newArrayList();
            assignedShards.put(shard.shardId(), shards);
        }
        assert  assertInstanceNotInList(shard, shards);
        shards.add(shard);
    }

    private boolean assertInstanceNotInList(ShardRouting shard, List<ShardRouting> shards) {
        for (ShardRouting s : shards) {
            assert s != shard;
        }
        return true;
    }

    private void assignedShardsRemove(ShardRouting shard) {
        final List<ShardRouting> replicaSet = assignedShards.get(shard.shardId());
        if (replicaSet != null) {
            final Iterator<ShardRouting> iterator = replicaSet.iterator();
            while(iterator.hasNext()) {
                // yes we check identity here
                if (shard == iterator.next()) {
                    iterator.remove();
                    return;
                }
            }
            assert false : "Illegal state";
        }
    }

    public boolean isKnown(DiscoveryNode node) {
        return nodesToShards.containsKey(node.getId());
    }

    public void addNode(DiscoveryNode node) {
        RoutingNode routingNode = new RoutingNode(node.id(), node);
        nodesToShards.put(routingNode.nodeId(), routingNode);
    }

    public RoutingNodeIterator routingNodeIter(String nodeId) {
        final RoutingNode routingNode = nodesToShards.get(nodeId);
        if (routingNode == null) {
            return null;
        }
        return new RoutingNodeIterator(routingNode);
    }

    public RoutingNode[] toArray() {
        return nodesToShards.values().toArray(new RoutingNode[nodesToShards.size()]);
    }

    public void reinitShadowPrimary(ShardRouting candidate) {
        if (candidate.relocating()) {
            cancelRelocation(candidate);
        }
        candidate.reinitializeShard();
        inactivePrimaryCount++;
        inactiveShardCount++;

    }

    public final static class UnassignedShards implements Iterable<ShardRouting>  {

        private final List<ShardRouting> unassigned;

        private int primaries = 0;
        private long transactionId = 0;
        private final UnassignedShards source;
        private final long sourceTransactionId;

        public UnassignedShards(UnassignedShards other) {
            source = other;
            sourceTransactionId = other.transactionId;
            unassigned = new ArrayList<>(other.unassigned);
            primaries = other.primaries;
        }

        public UnassignedShards() {
            unassigned = new ArrayList<>();
            source = null;
            sourceTransactionId = -1;
        }

        public void add(ShardRouting shardRouting) {
            if(shardRouting.primary()) {
                primaries++;
            }
            unassigned.add(shardRouting);
            transactionId++;
        }

        public void addAll(Collection<ShardRouting> mutableShardRoutings) {
            for (ShardRouting r : mutableShardRoutings) {
                add(r);
            }
        }

        public void sort(Comparator<ShardRouting> comparator) {
            CollectionUtil.timSort(unassigned, comparator);
        }

        public int size() {
            return unassigned.size();
        }

        public int numPrimaries() {
            return primaries;
        }

        @Override
        public Iterator<ShardRouting> iterator() {
            final Iterator<ShardRouting> iterator = unassigned.iterator();
            return new Iterator<ShardRouting>() {
                private ShardRouting current;
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public ShardRouting next() {
                    return current = iterator.next();
                }

                @Override
                public void remove() {
                    iterator.remove();
                    if (current.primary()) {
                        primaries--;
                    }
                    transactionId++;
                }
            };
        }

        public boolean isEmpty() {
            return unassigned.isEmpty();
        }

        public void shuffle() {
            Collections.shuffle(unassigned);
        }

        public void clear() {
            transactionId++;
            unassigned.clear();
            primaries = 0;
        }

        public void transactionEnd(UnassignedShards shards) {
           assert shards.source == this && shards.sourceTransactionId == transactionId :
                   "Expected ID: " + shards.sourceTransactionId + " actual: " + transactionId + " Expected Source: " + shards.source + " actual: " + this;
           transactionId++;
           this.unassigned.clear();
           this.unassigned.addAll(shards.unassigned);
           this.primaries = shards.primaries;
        }

        public UnassignedShards transactionBegin() {
            return new UnassignedShards(this);
        }

        public ShardRouting[] drain() {
            ShardRouting[] mutableShardRoutings = unassigned.toArray(new ShardRouting[unassigned.size()]);
            unassigned.clear();
            primaries = 0;
            transactionId++;
            return mutableShardRoutings;
        }
    }


    /**
     * Calculates RoutingNodes statistics by iterating over all {@link ShardRouting}s
     * in the cluster to ensure the book-keeping is correct.
     * For performance reasons, this should only be called from asserts
     *
     * @return this method always returns <code>true</code> or throws an assertion error. If assertion are not enabled
     *         this method does nothing.
     */
    public static boolean assertShardStats(RoutingNodes routingNodes) {
        boolean run = false;
        assert (run = true); // only run if assertions are enabled!
        if (!run) {
            return true;
        }
        int unassignedPrimaryCount = 0;
        int inactivePrimaryCount = 0;
        int inactiveShardCount = 0;
        int relocating = 0;
        final Set<ShardId> seenShards = newHashSet();
        Map<String, Integer> indicesAndShards = new HashMap<>();
        for (RoutingNode node : routingNodes) {
            for (ShardRouting shard : node) {
                if (!shard.active() && shard.relocatingNodeId() == null) {
                    if (!shard.relocating()) {
                        inactiveShardCount++;
                        if (shard.primary()) {
                            inactivePrimaryCount++;
                        }
                    }
                }
                if (shard.relocating()) {
                    relocating++;
                }
                seenShards.add(shard.shardId());
                Integer i = indicesAndShards.get(shard.index());
                if (i == null) {
                    i = shard.id();
                }
                indicesAndShards.put(shard.index(), Math.max(i, shard.id()));
            }
        }
        // Assert that the active shard routing are identical.
        Set<Map.Entry<String, Integer>> entries = indicesAndShards.entrySet();
        final List<ShardRouting> shards = newArrayList();
        for (Map.Entry<String, Integer> e : entries) {
            String index = e.getKey();
            for (int i = 0; i < e.getValue(); i++) {
                for (RoutingNode routingNode : routingNodes) {
                    for (ShardRouting shardRouting : routingNode) {
                        if (shardRouting.index().equals(index) && shardRouting.id() == i) {
                            shards.add(shardRouting);
                        }
                    }
                }
                List<ShardRouting> mutableShardRoutings = routingNodes.assignedShards(new ShardId(index, i));
                assert mutableShardRoutings.size() == shards.size();
                for (ShardRouting r : mutableShardRoutings) {
                    assert shards.contains(r);
                    shards.remove(r);
                }
                assert shards.isEmpty();
            }
        }

        for (ShardRouting shard : routingNodes.unassigned()) {
            if (shard.primary()) {
                unassignedPrimaryCount++;
            }
            seenShards.add(shard.shardId());
        }

        assert unassignedPrimaryCount == routingNodes.unassignedShards.numPrimaries() :
                "Unassigned primaries is [" + unassignedPrimaryCount + "] but RoutingNodes returned unassigned primaries [" + routingNodes.unassigned().numPrimaries() + "]";
        assert inactivePrimaryCount == routingNodes.inactivePrimaryCount :
                "Inactive Primary count [" + inactivePrimaryCount + "] but RoutingNodes returned inactive primaries [" + routingNodes.inactivePrimaryCount + "]";
        assert inactiveShardCount == routingNodes.inactiveShardCount :
                "Inactive Shard count [" + inactiveShardCount + "] but RoutingNodes returned inactive shards [" + routingNodes.inactiveShardCount + "]";
        assert routingNodes.getRelocatingShardCount() == relocating : "Relocating shards mismatch [" + routingNodes.getRelocatingShardCount() + "] but expected [" + relocating + "]";
        return true;
    }


    public class RoutingNodesIterator implements Iterator<RoutingNode>, Iterable<ShardRouting> {
        private RoutingNode current;
        private final Iterator<RoutingNode> delegate;

        public RoutingNodesIterator(Iterator<RoutingNode> iterator) {
            delegate = iterator;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public RoutingNode next() {
            return current = delegate.next();
        }

        public RoutingNodeIterator nodeShards() {
            return new RoutingNodeIterator(current);
        }

        @Override
        public void remove() {
           delegate.remove();
        }

        @Override
        public Iterator<ShardRouting> iterator() {
            return nodeShards();
        }
    }

    public final class RoutingNodeIterator implements Iterator<ShardRouting>, Iterable<ShardRouting> {
        private final RoutingNode iterable;
        private ShardRouting shard;
        private final Iterator<ShardRouting> delegate;

        public RoutingNodeIterator(RoutingNode iterable) {
            this.delegate = iterable.mutableIterator();
            this.iterable = iterable;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public ShardRouting next() {
            return shard = delegate.next();
        }

        @Override
        public void remove() {
            delegate.remove();
            RoutingNodes.this.remove(shard);
        }

        @Override
        public Iterator<ShardRouting> iterator() {
            return iterable.iterator();
        }

        public void moveToUnassigned(UnassignedInfo unassignedInfo) {
            remove();
            ShardRouting unassigned = new ShardRouting(shard); // protective copy of the mutable shard
            unassigned.moveToUnassigned(unassignedInfo);
            unassigned().add(unassigned);
        }
    }
}
