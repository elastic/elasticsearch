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
import com.google.common.collect.Iterators;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.shard.ShardId;

import java.util.*;
import java.util.function.Predicate;

/**
 * {@link RoutingNodes} represents a copy the routing information contained in
 * the {@link ClusterState cluster state}.
 */
public class RoutingNodes implements Iterable<RoutingNode> {

    private final MetaData metaData;

    private final ClusterBlocks blocks;

    private final RoutingTable routingTable;

    private final Map<String, RoutingNode> nodesToShards = new HashMap<>();

    private final UnassignedShards unassignedShards = new UnassignedShards(this);

    private final Map<ShardId, List<ShardRouting>> assignedShards = new HashMap<>();

    private final ImmutableOpenMap<String, ClusterState.Custom> customs;

    private final boolean readOnly;

    private int inactivePrimaryCount = 0;

    private int inactiveShardCount = 0;

    private int relocatingShards = 0;

    private final Map<String, ObjectIntHashMap<String>> nodesPerAttributeNames = new HashMap<>();

    public RoutingNodes(ClusterState clusterState) {
        this(clusterState, true);
    }

    public RoutingNodes(ClusterState clusterState, boolean readOnly) {
        this.readOnly = readOnly;
        this.metaData = clusterState.metaData();
        this.blocks = clusterState.blocks();
        this.routingTable = clusterState.routingTable();
        this.customs = clusterState.customs();

        Map<String, List<ShardRouting>> nodesToShards = new HashMap<>();
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
                            entries = new ArrayList<>();
                            nodesToShards.put(shard.currentNodeId(), entries);
                        }
                        final ShardRouting sr = getRouting(shard, readOnly);
                        entries.add(sr);
                        assignedShardsAdd(sr);
                        if (shard.relocating()) {
                            entries = nodesToShards.get(shard.relocatingNodeId());
                            relocatingShards++;
                            if (entries == null) {
                                entries = new ArrayList<>();
                                nodesToShards.put(shard.relocatingNodeId(), entries);
                            }
                            // add the counterpart shard with relocatingNodeId reflecting the source from which
                            // it's relocating from.
                            ShardRouting targetShardRouting = shard.buildTargetRelocatingShard();
                            if (readOnly) {
                                targetShardRouting.freeze();
                            }
                            entries.add(targetShardRouting);
                            assignedShardsAdd(targetShardRouting);
                        } else if (!shard.active()) { // shards that are initializing without being relocated
                            if (shard.primary()) {
                                inactivePrimaryCount++;
                            }
                            inactiveShardCount++;
                        }
                    } else {
                        final ShardRouting sr =  getRouting(shard, readOnly);
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

    private static ShardRouting getRouting(ShardRouting src, boolean readOnly) {
        if (readOnly) {
            src.freeze(); // we just freeze and reuse this instance if we are read only
        } else {
            src = new ShardRouting(src);
        }
        return src;
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

    public boolean hasUnassigned() {
        return !unassignedShards.isEmpty();
    }

    public UnassignedShards unassigned() {
        return this.unassignedShards;
    }

    public RoutingNodesIterator nodes() {
        return new RoutingNodesIterator(nodesToShards.values().iterator());
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
        List<ShardRouting> shards = new ArrayList<>();
        for (RoutingNode routingNode : this) {
            for (ShardRouting shardRouting : routingNode) {
                if (predicate.test(shardRouting)) {
                    shards.add(shardRouting);
                }
            }
        }
        return shards;
    }

    public List<ShardRouting> shardsWithState(ShardRoutingState... state) {
        // TODO these are used on tests only - move into utils class
        List<ShardRouting> shards = new ArrayList<>();
        for (RoutingNode routingNode : this) {
            shards.addAll(routingNode.shardsWithState(state));
        }
        for (ShardRoutingState s : state) {
            if (s == ShardRoutingState.UNASSIGNED) {
                unassigned().forEach(shards::add);
                break;
            }
        }
        return shards;
    }

    public List<ShardRouting> shardsWithState(String index, ShardRoutingState... state) {
        // TODO these are used on tests only - move into utils class
        List<ShardRouting> shards = new ArrayList<>();
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
     * Moves a shard from unassigned to initialize state
     */
    public void initialize(ShardRouting shard, String nodeId, long expectedSize) {
        ensureMutable();
        assert shard.unassigned() : shard;
        shard.initialize(nodeId, expectedSize);
        node(nodeId).add(shard);
        inactiveShardCount++;
        if (shard.primary()) {
            inactivePrimaryCount++;
        }
        assignedShardsAdd(shard);
    }

    /**
     * Relocate a shard to another node, adding the target initializing
     * shard as well as assigning it. And returning the target initializing
     * shard.
     */
    public ShardRouting relocate(ShardRouting shard, String nodeId, long expectedShardSize) {
        ensureMutable();
        relocatingShards++;
        shard.relocate(nodeId, expectedShardSize);
        ShardRouting target = shard.buildTargetRelocatingShard();
        node(target.currentNodeId()).add(target);
        assignedShardsAdd(target);
        return target;
    }

    /**
     * Mark a shard as started and adjusts internal statistics.
     */
    public void started(ShardRouting shard) {
        ensureMutable();
        assert !shard.active() : "expected an intializing shard " + shard;
        if (shard.relocatingNodeId() == null) {
            // if this is not a target shard for relocation, we need to update statistics
            inactiveShardCount--;
            if (shard.primary()) {
                inactivePrimaryCount--;
            }
        }
        shard.moveToStarted();
    }

    /**
     * Cancels a relocation of a shard that shard must relocating.
     */
    public void cancelRelocation(ShardRouting shard) {
        ensureMutable();
        relocatingShards--;
        shard.cancelRelocation();
    }

    /**
     * swaps the status of a shard, making replicas primary and vice versa.
     *
     * @param shards the shard to have its primary status swapped.
     */
    public void swapPrimaryFlag(ShardRouting... shards) {
        ensureMutable();
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
        ensureMutable();
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
            shards = new ArrayList<>();
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
        ensureMutable();
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
        ensureMutable();
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
        ensureMutable();
        if (candidate.relocating()) {
            cancelRelocation(candidate);
        }
        candidate.reinitializeShard();
        inactivePrimaryCount++;
        inactiveShardCount++;

    }

    public static final class UnassignedShards implements Iterable<ShardRouting>  {

        private final RoutingNodes nodes;
        private final List<ShardRouting> unassigned;
        private final List<ShardRouting> ignored;

        private int primaries = 0;
        private long transactionId = 0;
        private final UnassignedShards source;
        private final long sourceTransactionId;

        public UnassignedShards(UnassignedShards other) {
            this.nodes = other.nodes;
            source = other;
            sourceTransactionId = other.transactionId;
            unassigned = new ArrayList<>(other.unassigned);
            ignored = new ArrayList<>(other.ignored);
            primaries = other.primaries;
        }

        public UnassignedShards(RoutingNodes nodes) {
            this.nodes = nodes;
            unassigned = new ArrayList<>();
            ignored = new ArrayList<>();
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
        public UnassignedIterator iterator() {
            return new UnassignedIterator();
        }

        /**
         * The list of ignored unassigned shards (read only). The ignored unassigned shards
         * are not part of the formal unassigned list, but are kept around and used to build
         * back the list of unassigned shards as part of the routing table.
         */
        public List<ShardRouting> ignored() {
            return Collections.unmodifiableList(ignored);
        }

        /**
         * Adds a shard to the ignore unassigned list. Should be used with caution, typically,
         * the correct usage is to removeAndIgnore from the iterator.
         */
        public void ignoreShard(ShardRouting shard) {
            ignored.add(shard);
            transactionId++;
        }

        public class UnassignedIterator implements Iterator<ShardRouting> {

            private final Iterator<ShardRouting> iterator;
            private ShardRouting current;

            public UnassignedIterator() {
                this.iterator = unassigned.iterator();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public ShardRouting next() {
                return current = iterator.next();
            }

            /**
             * Initializes the current unassigned shard and moves it from the unassigned list.
             */
            public void initialize(String nodeId, long version, long expectedShardSize) {
                innerRemove();
                nodes.initialize(new ShardRouting(current, version), nodeId, expectedShardSize);
            }

            /**
             * Removes and ignores the unassigned shard (will be ignored for this run, but
             * will be added back to unassigned once the metadata is constructed again).
             */
            public void removeAndIgnore() {
                innerRemove();
                ignoreShard(current);
            }

            /**
             * Unsupported operation, just there for the interface. Use {@link #removeAndIgnore()} or
             * {@link #initialize(String, long, long)}.
             */
            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported in unassigned iterator, use removeAndIgnore or initialize");
            }

            private void innerRemove() {
                nodes.ensureMutable();
                iterator.remove();
                if (current.primary()) {
                    primaries--;
                }
                transactionId++;
            }
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
            ignored.clear();
            primaries = 0;
        }

        public void transactionEnd(UnassignedShards shards) {
            assert shards.source == this && shards.sourceTransactionId == transactionId :
                    "Expected ID: " + shards.sourceTransactionId + " actual: " + transactionId + " Expected Source: " + shards.source + " actual: " + this;
            transactionId++;
            this.unassigned.clear();
            this.unassigned.addAll(shards.unassigned);
            this.ignored.clear();
            this.ignored.addAll(shards.ignored);
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
        final Set<ShardId> seenShards = new HashSet<>();
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
        final List<ShardRouting> shards = new ArrayList<>();
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
        private boolean removed = false;

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
            removed = false;
            return shard = delegate.next();
        }

        @Override
        public void remove() {
            ensureMutable();
            delegate.remove();
            RoutingNodes.this.remove(shard);
            removed = true;
        }


        /** returns true if {@link #remove()} or {@link #moveToUnassigned(UnassignedInfo)} were called on the current shard */
        public boolean isRemoved() {
            return removed;
        }

        @Override
        public Iterator<ShardRouting> iterator() {
            return iterable.iterator();
        }

        public void moveToUnassigned(UnassignedInfo unassignedInfo) {
            ensureMutable();
            if (isRemoved() == false) {
                remove();
            }
            ShardRouting unassigned = new ShardRouting(shard); // protective copy of the mutable shard
            unassigned.moveToUnassigned(unassignedInfo);
            unassigned().add(unassigned);
        }

        public ShardRouting current() {
            return shard;
        }
    }

    private void ensureMutable() {
        if (readOnly) {
            throw new IllegalStateException("can't modify RoutingNodes - readonly");
        }
    }
}
