/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
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

    private final List<MutableShardRouting> unassigned = newArrayList();

    private final List<MutableShardRouting> ignoredUnassigned = newArrayList();

    private final Map<ShardId, List<MutableShardRouting>> replicaSets = newHashMap();

    private int unassignedPrimaryCount = 0;

    private int inactivePrimaryCount = 0;

    private int inactiveShardCount = 0;

    Set<ShardId> relocatingReplicaSets = new HashSet<ShardId>();
    
    private Set<ShardId> clearPostAllocationFlag;

    private final Map<String, ObjectIntOpenHashMap<String>> nodesPerAttributeNames = new HashMap<String, ObjectIntOpenHashMap<String>>();

    public RoutingNodes(ClusterState clusterState) {
        this.metaData = clusterState.metaData();
        this.blocks = clusterState.blocks();
        this.routingTable = clusterState.routingTable();

        Map<String, List<MutableShardRouting>> nodesToShards = newHashMap();
        // fill in the nodeToShards with the "live" nodes
        for (ObjectCursor<DiscoveryNode> cursor : clusterState.nodes().dataNodes().values()) {
            nodesToShards.put(cursor.value.id(), new ArrayList<MutableShardRouting>());
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
                        List<MutableShardRouting> entries = nodesToShards.get(shard.currentNodeId());
                        if (entries == null) {
                            entries = newArrayList();
                            nodesToShards.put(shard.currentNodeId(), entries);
                        }
                        MutableShardRouting sr = new MutableShardRouting(shard);
                        entries.add(sr);
                        addToReplicaSet(sr);
                        if (shard.relocating()) {
                            entries = nodesToShards.get(shard.relocatingNodeId());
                            relocatingReplicaSets.add(shard.shardId());
                            if (entries == null) {
                                entries = newArrayList();
                                nodesToShards.put(shard.relocatingNodeId(), entries);
                            }
                            // add the counterpart shard with relocatingNodeId reflecting the source from which
                            // it's relocating from.
                            sr = new MutableShardRouting(shard.index(), shard.id(), shard.relocatingNodeId(),
                                    shard.currentNodeId(), shard.primary(), ShardRoutingState.INITIALIZING, shard.version());
                            entries.add(sr);
                            addToReplicaSet(sr);
                        } else if (!shard.active()) { // shards that are initializing without being relocated
                            if (shard.primary()) {
                                inactivePrimaryCount++;
                            }
                            inactiveShardCount++;
                        }
                    } else {
                        MutableShardRouting sr = new MutableShardRouting(shard);
                        addToReplicaSet(sr);
                        unassigned.add(sr);
                        if (shard.primary()) {
                            unassignedPrimaryCount++;
                        }

                    }
                }
            }
        }
        for (Map.Entry<String, List<MutableShardRouting>> entry : nodesToShards.entrySet()) {
            String nodeId = entry.getKey();
            this.nodesToShards.put(nodeId, new RoutingNode(nodeId, clusterState.nodes().get(nodeId), entry.getValue()));
        }
    }

    @Override
    public Iterator<RoutingNode> iterator() {
        return nodesToShards.values().iterator();
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
        return !unassigned.isEmpty();
    }

    public List<MutableShardRouting> ignoredUnassigned() {
        return this.ignoredUnassigned;
    }

    public List<MutableShardRouting> unassigned() {
        return this.unassigned;
    }

    public List<MutableShardRouting> getUnassigned() {
        return unassigned();
    }

    public Map<String, RoutingNode> nodesToShards() {
        return nodesToShards;
    }

    public Map<String, RoutingNode> getNodesToShards() {
        return nodesToShards();
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

    public ObjectIntOpenHashMap<String> nodesPerAttributesCounts(String attributeName) {
        ObjectIntOpenHashMap<String> nodesPerAttributesCounts = nodesPerAttributeNames.get(attributeName);
        if (nodesPerAttributesCounts != null) {
            return nodesPerAttributesCounts;
        }
        nodesPerAttributesCounts = new ObjectIntOpenHashMap<String>();
        for (RoutingNode routingNode : this) {
            String attrValue = routingNode.node().attributes().get(attributeName);
            nodesPerAttributesCounts.addTo(attrValue, 1);
        }
        nodesPerAttributeNames.put(attributeName, nodesPerAttributesCounts);
        return nodesPerAttributesCounts;
    }

    public boolean hasUnassignedPrimaries() {
        return unassignedPrimaryCount > 0;
    }

    public boolean hasUnassignedShards() {
        return !unassigned.isEmpty();
    }

    public boolean hasInactivePrimaries() {
        return inactivePrimaryCount > 0;
    }

    public boolean hasInactiveShards() {
        return inactiveShardCount > 0;
    }

    public int getRelocatingShardCount() {
        return relocatingReplicaSets.size();
    }

    public MutableShardRouting findPrimaryForReplica(ShardRouting shard) {
        assert !shard.primary();
        MutableShardRouting primary = null;
        for (MutableShardRouting shardRouting : shardsRoutingFor(shard)) {
            if (shardRouting.primary()) {
                primary = shardRouting;
                break;
            }
        }
        assert primary != null;
        return primary;
    }

    public List<MutableShardRouting> shardsRoutingFor(ShardRouting shardRouting) {
        return shardsRoutingFor(shardRouting.index(), shardRouting.id());
    }

    public List<MutableShardRouting> shardsRoutingFor(String index, int shardId) {
        ShardId sid = new ShardId(index, shardId);
        List<MutableShardRouting> shards = replicaSetFor(sid);
        assert shards != null;
        // no need to check unassigned array, since the ShardRoutings are in the replica set.
        return shards;
    }

    public int numberOfShardsOfType(ShardRoutingState state) {
        int count = 0;
        for (RoutingNode routingNode : this) {
            count += routingNode.numberOfShardsWithState(state);
        }
        return count;
    }

    public List<MutableShardRouting> shards(Predicate<MutableShardRouting> predicate) {
        List<MutableShardRouting> shards = newArrayList();
        for (RoutingNode routingNode : this) {
            List<MutableShardRouting> nodeShards = routingNode.shards();
            for (int i = 0; i < nodeShards.size(); i++) {
                MutableShardRouting shardRouting = nodeShards.get(i);
                if (predicate.apply(shardRouting)) {
                    shards.add(shardRouting);
                }
            }
        }
        return shards;
    }

    public List<MutableShardRouting> shardsWithState(ShardRoutingState... state) {
        List<MutableShardRouting> shards = newArrayList();
        for (RoutingNode routingNode : this) {
            shards.addAll(routingNode.shardsWithState(state));
        }
        return shards;
    }

    public List<MutableShardRouting> shardsWithState(String index, ShardRoutingState... state) {
        List<MutableShardRouting> shards = newArrayList();
        for (RoutingNode routingNode : this) {
            shards.addAll(routingNode.shardsWithState(index, state));
        }
        return shards;
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder("routing_nodes:\n");
        for (RoutingNode routingNode : this) {
            sb.append(routingNode.prettyPrint());
        }
        sb.append("---- unassigned\n");
        for (MutableShardRouting shardEntry : unassigned) {
            sb.append("--------").append(shardEntry.shortSummary()).append('\n');
        }
        return sb.toString();
    }

    /**
     * calculates RoutingNodes statistics by iterating over all {@link MutableShardRouting}s
     * in the cluster to ensure the {@link RoutingManager} book-keeping is correct.
     * For performance reasons, this should only be called from test cases.
     *
     * @return true if all counts are the same, false if either of the book-keeping numbers is off.
     */
    public boolean assertShardStats() {
       int unassignedPrimaryCount = 0;
       int inactivePrimaryCount = 0;
       int inactiveShardCount = 0;
       int totalShards = 0;

       Set<ShardId> seenShards = newHashSet();

       for (RoutingNode node : this) {
           for (MutableShardRouting shard : node) {
               if (!shard.active()) {
                   if (!shard.relocating()) {
                       inactiveShardCount++;
                       if (shard.primary()){
                           inactivePrimaryCount++;
                       }
                   }
               }
               totalShards++;
               seenShards.add(shard.shardId());
           }
       }
       for (MutableShardRouting shard : unassigned) {
           if (shard.primary()) {
               unassignedPrimaryCount++;
           }
           totalShards++;
           seenShards.add(shard.shardId());
       }

       for (ShardId shardId : seenShards) {
           assert replicaSetFor(shardId) != null;
       }

       assert unassignedPrimaryCount == 0 || hasUnassignedPrimaries();
       assert inactivePrimaryCount == 0 || hasInactivePrimaries();
       assert inactiveShardCount == 0 || hasInactiveShards();
       assert hasUnassignedPrimaries() || unassignedPrimaryCount == 0;
       assert hasInactivePrimaries() || inactivePrimaryCount == 0;
       assert hasInactiveShards() || inactiveShardCount == 0;

       return true;
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
    public void assignShardToNode(MutableShardRouting shard, String nodeId) {

        // state will not change if the shard is already initializing.
        ShardRoutingState oldState = shard.state();
        
        shard.assignToNode(nodeId);
        node(nodeId).add(shard);

        if (oldState == ShardRoutingState.UNASSIGNED) {
            inactiveShardCount++;
            if (shard.primary()) {
                unassignedPrimaryCount--;
                inactivePrimaryCount++;
            }
        }
        if (shard.state() == ShardRoutingState.RELOCATING) {
            // this a HashSet. double add no worry.
            relocatingReplicaSets.add(shard.shardId()); 
        }
        // possibly double/triple adding it to a replica set doesn't matter
        // but make sure we know about the shard.
        addToReplicaSet(shard);
    }

    /**
     * Relocate a shard to another node.
     *
     * STARTED => RELOCATING
     *
     * @param shard the shard to relocate
     * @param nodeId the node to relocate to
     */
    public void relocateShard(MutableShardRouting shard, String nodeId) {
        relocatingReplicaSets.add(shard.shardId());
        shard.relocate(nodeId);
    }

    /**
     * Cancels the relocation of a shard.
     *
     * RELOCATING => STARTED
     *
     * @param shard the shard that was relocating previously and now should be started again.
     */
    public void cancelRelocationForShard(MutableShardRouting shard) {
        relocatingReplicaSets.remove(shard.shardId());
        shard.cancelRelocation();
    }

    /**
     * Unassigns shard from a node.
     * Both relocating and started shards that are deallocated need a new 
     * primary elected.
     *
     * RELOCATING   => null
     * STARTED      => null
     * INITIALIZING => null
     *
     * @param shard the shard to be unassigned.
     */
    public void deassignShard(MutableShardRouting shard) {
        if (shard.state() == ShardRoutingState.RELOCATING) {
            cancelRelocationForShard(shard);
        }
        if (shard.primary())
            unassignedPrimaryCount++;
        shard.deassignNode();
    }

    /**
     * Mark a shard as started.
     * Decreases the counters and marks a replication complete or failed,
     * which is the same for accounting in this class.
     *
     * INITIALIZING => STARTED
     * RELOCATIng   => STARTED
     *
     * @param shard the shard to be marked as started
     */
    public void markShardStarted(MutableShardRouting shard) {
        if (!relocatingReplicaSets.contains(shard.shardId()) && shard.state() == ShardRoutingState.INITIALIZING) {
            inactiveShardCount--;
            if (shard.primary()) {
                inactivePrimaryCount--;
            }
        }
        if (shard.state() == ShardRoutingState.INITIALIZING 
             && shard.relocatingNodeId() != null) {
            relocatingReplicaSets.remove(shard.shardId());
        }
        shard.moveToStarted();
    }

    /**
     * Return a list of shards belonging to a replica set
     * 
     * @param shard the shard for which to retrieve the replica set
     * @return an unmodifiable List of the replica set
     */
    public List<MutableShardRouting> replicaSetFor(MutableShardRouting shard) {
        return replicaSetFor(shard.shardId());
    }

    /**
     * Return a list of shards belonging to a replica set
     * 
     * @param shardId the {@link ShardId} for which to retrieve the replica set
     * @return an unmodifiable List of the replica set
     */
    public List<MutableShardRouting> replicaSetFor(ShardId shardId) {
        List<MutableShardRouting> replicaSet = replicaSets.get(shardId);
        assert replicaSet != null;
        return Collections.unmodifiableList(replicaSet);
    }

    /**
     * Let this class know about a shard, which it then sorts into 
     * its replica set. Package private as only {@link RoutingNodes} 
     * should notify this class of shards during initialization.
     *
     * @param shard the shard to be sorted into its replica set
     */
    private void addToReplicaSet(MutableShardRouting shard) {
        List<MutableShardRouting> replicaSet = replicaSets.get(shard.shardId());
        if (replicaSet == null) {
            replicaSet = new ArrayList<MutableShardRouting>();
            replicaSets.put(shard.shardId(), replicaSet);
        }
        replicaSet.add(shard);
    }

    /**
     * marks a replica set as relocating. 
     *
     * @param shard a member of the relocating replica set
     */
    private void markRelocating(MutableShardRouting shard) {
        relocatingReplicaSets.add(shard.shardId());
    }

    /**
     * swaps the status of a shard, making replicas primary and vice versa.
     * 
     * @param shard the shard to have its primary status swapped.
     */
    public void changePrimaryStatusForShard(MutableShardRouting... shards) {
        for (MutableShardRouting shard : shards) {
            if (shard.primary()) {
                shard.moveFromPrimary();
            } else {
                shard.moveToPrimary();
            }
        }
    }
}
