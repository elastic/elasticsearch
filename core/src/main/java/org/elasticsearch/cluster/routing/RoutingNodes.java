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
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;

/**
 * {@link RoutingNodes} represents a copy the routing information contained in
 * the {@link ClusterState cluster state}.
 */
public class RoutingNodes implements Iterable<RoutingNode> {

    private final Map<String, RoutingNode> nodesToShards = new HashMap<>();

    private final UnassignedShards unassignedShards = new UnassignedShards(this);

    private final Map<ShardId, List<ShardRouting>> assignedShards = new HashMap<>();

    private final boolean readOnly;

    private int inactivePrimaryCount = 0;

    private int inactiveShardCount = 0;

    private int relocatingShards = 0;

    private final Map<String, ObjectIntHashMap<String>> nodesPerAttributeNames = new HashMap<>();
    private final Map<String, Recoveries> recoveriesPerNode = new HashMap<>();

    public RoutingNodes(ClusterState clusterState) {
        this(clusterState, true);
    }

    public RoutingNodes(ClusterState clusterState, boolean readOnly) {
        this.readOnly = readOnly;
        final RoutingTable routingTable = clusterState.routingTable();

        Map<String, LinkedHashMap<ShardId, ShardRouting>> nodesToShards = new HashMap<>();
        // fill in the nodeToShards with the "live" nodes
        for (ObjectCursor<DiscoveryNode> cursor : clusterState.nodes().getDataNodes().values()) {
            nodesToShards.put(cursor.value.getId(), new LinkedHashMap<>()); // LinkedHashMap to preserve order
        }

        // fill in the inverse of node -> shards allocated
        // also fill replicaSet information
        for (ObjectCursor<IndexRoutingTable> indexRoutingTable : routingTable.indicesRouting().values()) {
            for (IndexShardRoutingTable indexShard : indexRoutingTable.value) {
                assert indexShard.primary != null;
                for (ShardRouting shard : indexShard) {
                    // to get all the shards belonging to an index, including the replicas,
                    // we define a replica set and keep track of it. A replica set is identified
                    // by the ShardId, as this is common for primary and replicas.
                    // A replica Set might have one (and not more) replicas with the state of RELOCATING.
                    if (shard.assignedToNode()) {
                        Map<ShardId, ShardRouting> entries = nodesToShards.computeIfAbsent(shard.currentNodeId(),
                            k -> new LinkedHashMap<>()); // LinkedHashMap to preserve order
                        ShardRouting previousValue = entries.put(shard.shardId(), shard);
                        if (previousValue != null) {
                            throw new IllegalArgumentException("Cannot have two different shards with same shard id on same node");
                        }
                        assignedShardsAdd(shard);
                        if (shard.relocating()) {
                            relocatingShards++;
                            entries = nodesToShards.computeIfAbsent(shard.relocatingNodeId(),
                                k -> new LinkedHashMap<>()); // LinkedHashMap to preserve order
                            // add the counterpart shard with relocatingNodeId reflecting the source from which
                            // it's relocating from.
                            ShardRouting targetShardRouting = shard.getTargetRelocatingShard();
                            addInitialRecovery(targetShardRouting, indexShard.primary);
                            previousValue = entries.put(targetShardRouting.shardId(), targetShardRouting);
                            if (previousValue != null) {
                                throw new IllegalArgumentException("Cannot have two different shards with same shard id on same node");
                            }
                            assignedShardsAdd(targetShardRouting);
                        } else if (shard.initializing()) {
                            if (shard.primary()) {
                                inactivePrimaryCount++;
                            }
                            inactiveShardCount++;
                            addInitialRecovery(shard, indexShard.primary);
                        }
                    } else {
                        unassignedShards.add(shard);
                    }
                }
            }
        }
        for (Map.Entry<String, LinkedHashMap<ShardId, ShardRouting>> entry : nodesToShards.entrySet()) {
            String nodeId = entry.getKey();
            this.nodesToShards.put(nodeId, new RoutingNode(nodeId, clusterState.nodes().get(nodeId), entry.getValue()));
        }
    }

    private void addRecovery(ShardRouting routing) {
        updateRecoveryCounts(routing, true, findAssignedPrimaryIfPeerRecovery(routing));
    }

    private void removeRecovery(ShardRouting routing) {
        updateRecoveryCounts(routing, false, findAssignedPrimaryIfPeerRecovery(routing));
    }

    private void addInitialRecovery(ShardRouting routing, ShardRouting initialPrimaryShard) {
        updateRecoveryCounts(routing, true, initialPrimaryShard);
    }

    private void updateRecoveryCounts(final ShardRouting routing, final boolean increment, @Nullable final ShardRouting primary) {
        final int howMany = increment ? 1 : -1;
        assert routing.initializing() : "routing must be initializing: " + routing;
        // TODO: check primary == null || primary.active() after all tests properly add ReplicaAfterPrimaryActiveAllocationDecider
        assert primary == null || primary.assignedToNode() :
            "shard is initializing but its primary is not assigned to a node";

        Recoveries.getOrAdd(recoveriesPerNode, routing.currentNodeId()).addIncoming(howMany);

        if (routing.isPeerRecovery()) {
            // add/remove corresponding outgoing recovery on node with primary shard
            if (primary == null) {
                throw new IllegalStateException("shard is peer recovering but primary is unassigned");
            }
            Recoveries.getOrAdd(recoveriesPerNode, primary.currentNodeId()).addOutgoing(howMany);

            if (increment == false && routing.primary() && routing.relocatingNodeId() != null) {
                // primary is done relocating, move non-primary recoveries from old primary to new primary
                int numRecoveringReplicas = 0;
                for (ShardRouting assigned : assignedShards(routing.shardId())) {
                    if (assigned.primary() == false && assigned.isPeerRecovery()) {
                        numRecoveringReplicas++;
                    }
                }
                recoveriesPerNode.get(routing.relocatingNodeId()).addOutgoing(-numRecoveringReplicas);
                recoveriesPerNode.get(routing.currentNodeId()).addOutgoing(numRecoveringReplicas);
            }
        }
    }

    public int getIncomingRecoveries(String nodeId) {
        return recoveriesPerNode.getOrDefault(nodeId, Recoveries.EMPTY).getIncoming();
    }

    public int getOutgoingRecoveries(String nodeId) {
        return recoveriesPerNode.getOrDefault(nodeId, Recoveries.EMPTY).getOutgoing();
    }

    @Nullable
    private ShardRouting findAssignedPrimaryIfPeerRecovery(ShardRouting routing) {
        ShardRouting primary = null;
        if (routing.isPeerRecovery()) {
            List<ShardRouting> shardRoutings = assignedShards.get(routing.shardId());
            if (shardRoutings != null) {
                for (ShardRouting shardRouting : shardRoutings) {
                    if (shardRouting.primary()) {
                        if (shardRouting.active()) {
                            return shardRouting;
                        } else if (primary == null) {
                            primary = shardRouting;
                        } else if (primary.relocatingNodeId() != null) {
                            primary = shardRouting;
                        }
                    }
                }
            }
        }
        return primary;
    }

    @Override
    public Iterator<RoutingNode> iterator() {
        return Collections.unmodifiableCollection(nodesToShards.values()).iterator();
    }

    public Iterator<RoutingNode> mutableIterator() {
        return nodesToShards.values().iterator();
    }

    public UnassignedShards unassigned() {
        return this.unassignedShards;
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
            String attrValue = routingNode.node().getAttributes().get(attributeName);
            nodesPerAttributesCounts.addTo(attrValue, 1);
        }
        nodesPerAttributeNames.put(attributeName, nodesPerAttributesCounts);
        return nodesPerAttributesCounts;
    }

    /**
     * Returns <code>true</code> iff this {@link RoutingNodes} instance has any unassigned primaries even if the
     * primaries are marked as temporarily ignored.
     */
    public boolean hasUnassignedPrimaries() {
        return unassignedShards.getNumPrimaries() + unassignedShards.getNumIgnoredPrimaries() > 0;
    }

    /**
     * Returns <code>true</code> iff this {@link RoutingNodes} instance has any unassigned shards even if the
     * shards are marked as temporarily ignored.
     * @see UnassignedShards#isEmpty()
     * @see UnassignedShards#isIgnoredEmpty()
     */
    public boolean hasUnassignedShards() {
        return unassignedShards.isEmpty() == false || unassignedShards.isIgnoredEmpty() == false;
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
     * Returns all shards that are not in the state UNASSIGNED with the same shard
     * ID as the given shard.
     */
    public List<ShardRouting> assignedShards(ShardId shardId) {
        final List<ShardRouting> replicaSet = assignedShards.get(shardId);
        return replicaSet == null ? EMPTY : Collections.unmodifiableList(replicaSet);
    }

    @Nullable
    public ShardRouting getByAllocationId(ShardId shardId, String allocationId) {
        final List<ShardRouting> replicaSet = assignedShards.get(shardId);
        if (replicaSet == null) {
            return null;
        }
        for (ShardRouting shardRouting : replicaSet) {
            if (shardRouting.allocationId().getId().equals(allocationId)) {
                return shardRouting;
            }
        }
        return null;
    }

    /**
     * Returns the active primary shard for the given shard id or <code>null</code> if
     * no primary is found or the primary is not active.
     */
    public ShardRouting activePrimary(ShardId shardId) {
        for (ShardRouting shardRouting : assignedShards(shardId)) {
            if (shardRouting.primary() && shardRouting.active()) {
                return shardRouting;
            }
        }
        return null;
    }

    /**
     * Returns one active replica shard for the given shard id or <code>null</code> if
     * no active replica is found.
     */
    public ShardRouting activeReplica(ShardId shardId) {
        for (ShardRouting shardRouting : assignedShards(shardId)) {
            if (!shardRouting.primary() && shardRouting.active()) {
                return shardRouting;
            }
        }
        return null;
    }

    /**
     * Returns <code>true</code> iff all replicas are active for the given shard routing. Otherwise <code>false</code>
     */
    public boolean allReplicasActive(ShardId shardId, MetaData metaData) {
        final List<ShardRouting> shards = assignedShards(shardId);
        if (shards.isEmpty() || shards.size() < metaData.getIndexSafe(shardId.getIndex()).getNumberOfReplicas() + 1) {
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
                    if (unassignedShard.index().getName().equals(index)) {
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
     *
     * @param existingAllocationId allocation id to use. If null, a fresh allocation id is generated.
     * @return                     the initialized shard
     */
    public ShardRouting initialize(ShardRouting shard, String nodeId, @Nullable String existingAllocationId, long expectedSize) {
        ensureMutable();
        assert shard.unassigned() : "expected an unassigned shard " + shard;
        ShardRouting initializedShard = shard.initialize(nodeId, existingAllocationId, expectedSize);
        node(nodeId).add(initializedShard);
        inactiveShardCount++;
        if (initializedShard.primary()) {
            inactivePrimaryCount++;
        }
        addRecovery(initializedShard);
        assignedShardsAdd(initializedShard);
        return initializedShard;
    }

    /**
     * Relocate a shard to another node, adding the target initializing
     * shard as well as assigning it.
     *
     * @return pair of source relocating and target initializing shards.
     */
    public Tuple<ShardRouting,ShardRouting> relocate(ShardRouting shard, String nodeId, long expectedShardSize) {
        ensureMutable();
        relocatingShards++;
        ShardRouting source = shard.relocate(nodeId, expectedShardSize);
        ShardRouting target = source.getTargetRelocatingShard();
        updateAssigned(shard, source);
        node(target.currentNodeId()).add(target);
        assignedShardsAdd(target);
        addRecovery(target);
        return Tuple.tuple(source, target);
    }

    /**
     * Mark a shard as started and adjusts internal statistics.
     *
     * @return the started shard
     */
    public ShardRouting started(ShardRouting shard) {
        ensureMutable();
        assert shard.initializing() : "expected an initializing shard " + shard;
        if (shard.relocatingNodeId() == null) {
            // if this is not a target shard for relocation, we need to update statistics
            inactiveShardCount--;
            if (shard.primary()) {
                inactivePrimaryCount--;
            }
        }
        removeRecovery(shard);
        ShardRouting startedShard = shard.moveToStarted();
        updateAssigned(shard, startedShard);
        return startedShard;
    }



    /**
     * Cancels a relocation of a shard that shard must relocating.
     *
     * @return the shard after cancelling relocation
     */
    public ShardRouting cancelRelocation(ShardRouting shard) {
        ensureMutable();
        relocatingShards--;
        ShardRouting cancelledShard = shard.cancelRelocation();
        updateAssigned(shard, cancelledShard);
        return cancelledShard;
    }

    /**
     * moves the assigned replica shard to primary.
     *
     * @param replicaShard the replica shard to be promoted to primary
     * @return             the resulting primary shard
     */
    public ShardRouting promoteAssignedReplicaShardToPrimary(ShardRouting replicaShard) {
        ensureMutable();
        assert replicaShard.unassigned() == false : "unassigned shard cannot be promoted to primary: " + replicaShard;
        assert replicaShard.primary() == false : "primary shard cannot be promoted to primary: " + replicaShard;
        ShardRouting primaryShard = replicaShard.moveToPrimary();
        updateAssigned(replicaShard, primaryShard);
        return primaryShard;
    }

    private static final List<ShardRouting> EMPTY = Collections.emptyList();

    /**
     * Cancels the give shard from the Routing nodes internal statistics and cancels
     * the relocation if the shard is relocating.
     */
    public void remove(ShardRouting shard) {
        ensureMutable();
        assert shard.unassigned() == false : "only assigned shards can be removed here (" + shard + ")";
        node(shard.currentNodeId()).remove(shard);
        if (shard.initializing() && shard.relocatingNodeId() == null) {
            inactiveShardCount--;
            assert inactiveShardCount >= 0;
            if (shard.primary()) {
                inactivePrimaryCount--;
            }
        } else if (shard.relocating()) {
            shard = cancelRelocation(shard);
        }
        assignedShardsRemove(shard);
        if (shard.initializing()) {
            removeRecovery(shard);
        }
    }

    /**
     * Removes relocation source of an initializing non-primary shard. This allows the replica shard to continue recovery from
     * the primary even though its non-primary relocation source has failed.
     */
    public ShardRouting removeRelocationSource(ShardRouting shard) {
        assert shard.isRelocationTarget() : "only relocation target shards can have their relocation source removed (" + shard + ")";
        ensureMutable();
        ShardRouting relocationMarkerRemoved = shard.removeRelocationSource();
        updateAssigned(shard, relocationMarkerRemoved);
        inactiveShardCount++; // relocation targets are not counted as inactive shards whereas initializing shards are
        return relocationMarkerRemoved;
    }

    private void assignedShardsAdd(ShardRouting shard) {
        assert shard.unassigned() == false : "unassigned shard " + shard + " cannot be added to list of assigned shards";
        List<ShardRouting> shards = assignedShards.computeIfAbsent(shard.shardId(), k -> new ArrayList<>());
        assert assertInstanceNotInList(shard, shards) : "shard " + shard + " cannot appear twice in list of assigned shards";
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
        }
        assert false : "No shard found to remove";
    }

    public ShardRouting reinitShadowPrimary(ShardRouting candidate) {
        ensureMutable();
        if (candidate.relocating()) {
            cancelRelocation(candidate);
        }
        ShardRouting reinitializedShard = candidate.reinitializeShard();
        updateAssigned(candidate, reinitializedShard);
        inactivePrimaryCount++;
        inactiveShardCount++;
        return reinitializedShard;
    }

    private void updateAssigned(ShardRouting oldShard, ShardRouting newShard) {
        assert oldShard.shardId().equals(newShard.shardId()) :
            "can only update " + oldShard + " by shard with same shard id but was " + newShard;
        assert oldShard.unassigned() == false && newShard.unassigned() == false :
            "only assigned shards can be updated in list of assigned shards (prev: " + oldShard + ", new: " + newShard + ")";
        assert oldShard.currentNodeId().equals(newShard.currentNodeId()) : "shard to update " + oldShard +
            " can only update " + oldShard + " by shard assigned to same node but was " + newShard;
        node(oldShard.currentNodeId()).update(oldShard, newShard);
        List<ShardRouting> shardsWithMatchingShardId = assignedShards.computeIfAbsent(oldShard.shardId(), k -> new ArrayList<>());
        int previousShardIndex = shardsWithMatchingShardId.indexOf(oldShard);
        assert previousShardIndex >= 0 : "shard to update " + oldShard + " does not exist in list of assigned shards";
        shardsWithMatchingShardId.set(previousShardIndex, newShard);
    }

    public ShardRouting moveToUnassigned(ShardRouting shard, UnassignedInfo unassignedInfo) {
        ensureMutable();
        assert shard.unassigned() == false : "only assigned shards can be moved to unassigned (" + shard + ")";
        remove(shard);
        ShardRouting unassigned = shard.moveToUnassigned(unassignedInfo);
        unassignedShards.add(unassigned);
        return unassigned;
    }

    /**
     * Returns the number of routing nodes
     */
    public int size() {
        return nodesToShards.size();
    }

    public static final class UnassignedShards implements Iterable<ShardRouting>  {

        private final RoutingNodes nodes;
        private final List<ShardRouting> unassigned;
        private final List<ShardRouting> ignored;

        private int primaries = 0;
        private int ignoredPrimaries = 0;

        public UnassignedShards(RoutingNodes nodes) {
            this.nodes = nodes;
            unassigned = new ArrayList<>();
            ignored = new ArrayList<>();
        }

        public void add(ShardRouting shardRouting) {
            if(shardRouting.primary()) {
                primaries++;
            }
            unassigned.add(shardRouting);
        }

        public void sort(Comparator<ShardRouting> comparator) {
            CollectionUtil.timSort(unassigned, comparator);
        }

        /**
         * Returns the size of the non-ignored unassigned shards
         */
        public int size() { return unassigned.size(); }

        /**
         * Returns the size of the temporarily marked as ignored unassigned shards
         */
        public int ignoredSize() { return ignored.size(); }

        /**
         * Returns the number of non-ignored unassigned primaries
         */
        public int getNumPrimaries() {
            return primaries;
        }

        /**
         * Returns the number of temporarily marked as ignored unassigned primaries
         */
        public int getNumIgnoredPrimaries() { return ignoredPrimaries; }

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
         * Marks a shard as temporarily ignored and adds it to the ignore unassigned list.
         * Should be used with caution, typically,
         * the correct usage is to removeAndIgnore from the iterator.
         * @see #ignored()
         * @see UnassignedIterator#removeAndIgnore(AllocationStatus)
         * @see #isIgnoredEmpty()
         * @return true iff the decision caused a change to the unassigned info
         */
        public boolean ignoreShard(ShardRouting shard, AllocationStatus allocationStatus) {
            boolean changed = false;
            if (shard.primary()) {
                ignoredPrimaries++;
                UnassignedInfo currInfo = shard.unassignedInfo();
                assert currInfo != null;
                if (allocationStatus.equals(currInfo.getLastAllocationStatus()) == false) {
                    UnassignedInfo newInfo = new UnassignedInfo(currInfo.getReason(), currInfo.getMessage(), currInfo.getFailure(),
                                                                currInfo.getNumFailedAllocations(), currInfo.getUnassignedTimeInNanos(),
                                                                currInfo.getUnassignedTimeInMillis(), currInfo.isDelayed(),
                                                                allocationStatus);
                    shard = shard.updateUnassignedInfo(newInfo);
                    changed = true;
                }
            }
            ignored.add(shard);
            return changed;
        }

        public class UnassignedIterator implements Iterator<ShardRouting> {

            private final ListIterator<ShardRouting> iterator;
            private ShardRouting current;

            public UnassignedIterator() {
                this.iterator = unassigned.listIterator();
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
             *
             * @param existingAllocationId allocation id to use. If null, a fresh allocation id is generated.
             */
            public ShardRouting initialize(String nodeId, @Nullable String existingAllocationId, long expectedShardSize) {
                innerRemove();
                return nodes.initialize(current, nodeId, existingAllocationId, expectedShardSize);
            }

            /**
             * Removes and ignores the unassigned shard (will be ignored for this run, but
             * will be added back to unassigned once the metadata is constructed again).
             * Typically this is used when an allocation decision prevents a shard from being allocated such
             * that subsequent consumers of this API won't try to allocate this shard again.
             *
             * @param attempt the result of the allocation attempt
             * @return true iff the decision caused an update to the unassigned info
             */
            public boolean removeAndIgnore(AllocationStatus attempt) {
                innerRemove();
                return ignoreShard(current, attempt);
            }

            private void updateShardRouting(ShardRouting shardRouting) {
                current = shardRouting;
                iterator.set(shardRouting);
            }

            /**
             * updates the unassigned info on the current unassigned shard
             *
             * @param  unassignedInfo the new unassigned info to use
             * @return the shard with unassigned info updated
             */
            public ShardRouting updateUnassignedInfo(UnassignedInfo unassignedInfo) {
                ShardRouting updatedShardRouting = current.updateUnassignedInfo(unassignedInfo);
                updateShardRouting(updatedShardRouting);
                return updatedShardRouting;
            }

            /**
             * marks the current primary shard as replica
             *
             * @return the shard with primary status swapped
             */
            public ShardRouting demotePrimaryToReplicaShard() {
                assert current.primary() : "non-primary shard " + current + " cannot be demoted";
                updateShardRouting(current.moveFromPrimary());
                primaries--;
                return current;
            }

            /**
             * Unsupported operation, just there for the interface. Use {@link #removeAndIgnore(AllocationStatus)} or
             * {@link #initialize(String, String, long)}.
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
            }
        }

        /**
         * Returns <code>true</code> iff this collection contains one or more non-ignored unassigned shards.
         */
        public boolean isEmpty() {
            return unassigned.isEmpty();
        }

        /**
         * Returns <code>true</code> iff any unassigned shards are marked as temporarily ignored.
         * @see UnassignedShards#ignoreShard(ShardRouting, AllocationStatus)
         * @see UnassignedIterator#removeAndIgnore(AllocationStatus)
         */
        public boolean isIgnoredEmpty() {
            return ignored.isEmpty();
        }

        public void shuffle() {
            Randomness.shuffle(unassigned);
        }

        /**
         * Drains all unassigned shards and returns it.
         * This method will not drain ignored shards.
         */
        public ShardRouting[] drain() {
            ShardRouting[] mutableShardRoutings = unassigned.toArray(new ShardRouting[unassigned.size()]);
            unassigned.clear();
            primaries = 0;
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
        int unassignedIgnoredPrimaryCount = 0;
        int inactivePrimaryCount = 0;
        int inactiveShardCount = 0;
        int relocating = 0;
        Map<Index, Integer> indicesAndShards = new HashMap<>();
        for (RoutingNode node : routingNodes) {
            for (ShardRouting shard : node) {
                if (shard.initializing() && shard.relocatingNodeId() == null) {
                    inactiveShardCount++;
                    if (shard.primary()) {
                        inactivePrimaryCount++;
                    }
                }
                if (shard.relocating()) {
                    relocating++;
                }
                Integer i = indicesAndShards.get(shard.index());
                if (i == null) {
                    i = shard.id();
                }
                indicesAndShards.put(shard.index(), Math.max(i, shard.id()));
            }
        }
        // Assert that the active shard routing are identical.
        Set<Map.Entry<Index, Integer>> entries = indicesAndShards.entrySet();
        final List<ShardRouting> shards = new ArrayList<>();
        for (Map.Entry<Index, Integer> e : entries) {
            Index index = e.getKey();
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
        }

        for (ShardRouting shard : routingNodes.unassigned().ignored()) {
            if (shard.primary()) {
                unassignedIgnoredPrimaryCount++;
            }
        }

        for (Map.Entry<String, Recoveries> recoveries : routingNodes.recoveriesPerNode.entrySet()) {
            String node = recoveries.getKey();
            final Recoveries value = recoveries.getValue();
            int incoming = 0;
            int outgoing = 0;
            RoutingNode routingNode = routingNodes.nodesToShards.get(node);
            if (routingNode != null) { // node might have dropped out of the cluster
                for (ShardRouting routing : routingNode) {
                    if (routing.initializing()) {
                        incoming++;
                    }
                    if (routing.primary() && routing.isPeerRecovery() == false) {
                        for (ShardRouting assigned : routingNodes.assignedShards.get(routing.shardId())) {
                            if (assigned.isPeerRecovery()) {
                                outgoing++;
                            }
                        }
                    }
                }
            }
            assert incoming == value.incoming : incoming + " != " + value.incoming + " node: " + routingNode;
            assert outgoing == value.outgoing : outgoing + " != " + value.outgoing + " node: " + routingNode;
        }


        assert unassignedPrimaryCount == routingNodes.unassignedShards.getNumPrimaries() :
                "Unassigned primaries is [" + unassignedPrimaryCount + "] but RoutingNodes returned unassigned primaries [" + routingNodes.unassigned().getNumPrimaries() + "]";
        assert unassignedIgnoredPrimaryCount == routingNodes.unassignedShards.getNumIgnoredPrimaries() :
                "Unassigned ignored primaries is [" + unassignedIgnoredPrimaryCount + "] but RoutingNodes returned unassigned ignored primaries [" + routingNodes.unassigned().getNumIgnoredPrimaries() + "]";
        assert inactivePrimaryCount == routingNodes.inactivePrimaryCount :
                "Inactive Primary count [" + inactivePrimaryCount + "] but RoutingNodes returned inactive primaries [" + routingNodes.inactivePrimaryCount + "]";
        assert inactiveShardCount == routingNodes.inactiveShardCount :
                "Inactive Shard count [" + inactiveShardCount + "] but RoutingNodes returned inactive shards [" + routingNodes.inactiveShardCount + "]";
        assert routingNodes.getRelocatingShardCount() == relocating : "Relocating shards mismatch [" + routingNodes.getRelocatingShardCount() + "] but expected [" + relocating + "]";

        return true;
    }

    private void ensureMutable() {
        if (readOnly) {
            throw new IllegalStateException("can't modify RoutingNodes - readonly");
        }
    }

    /**
     * Creates an iterator over shards interleaving between nodes: The iterator returns the first shard from
     * the first node, then the first shard of the second node, etc. until one shard from each node has been returned.
     * The iterator then resumes on the first node by returning the second shard and continues until all shards from
     * all the nodes have been returned.
     */
    public Iterator<ShardRouting> nodeInterleavedShardIterator() {
        final Queue<Iterator<ShardRouting>> queue = new ArrayDeque<>();
        for (Map.Entry<String, RoutingNode> entry : nodesToShards.entrySet()) {
            queue.add(entry.getValue().copyShards().iterator());
        }
        return new Iterator<ShardRouting>() {
            public boolean hasNext() {
                while (!queue.isEmpty()) {
                    if (queue.peek().hasNext()) {
                        return true;
                    }
                    queue.poll();
                }
                return false;
            }

            public ShardRouting next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                Iterator<ShardRouting> iter = queue.poll();
                ShardRouting result = iter.next();
                queue.offer(iter);
                return result;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static final class Recoveries {
        private static final Recoveries EMPTY = new Recoveries();
        private int incoming = 0;
        private int outgoing = 0;

        int getTotal() {
            return incoming + outgoing;
        }

        void addOutgoing(int howMany) {
            assert outgoing + howMany >= 0 : outgoing + howMany+ " must be >= 0";
            outgoing += howMany;
        }

        void addIncoming(int howMany) {
            assert incoming + howMany >= 0 : incoming + howMany+ " must be >= 0";
            incoming += howMany;
        }

        int getOutgoing() {
            return outgoing;
        }

        int getIncoming() {
            return incoming;
        }

        public static Recoveries getOrAdd(Map<String, Recoveries> map, String key) {
            Recoveries recoveries = map.get(key);
            if (recoveries == null) {
                recoveries = new Recoveries();
                map.put(key, recoveries);
            }
            return recoveries;
        }
     }
}
