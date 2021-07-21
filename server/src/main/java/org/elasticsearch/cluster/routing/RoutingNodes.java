/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.Assertions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * {@link RoutingNodes} represents a copy the routing information contained in the {@link ClusterState cluster state}.
 * It can be either initialized as mutable or immutable (see {@link #RoutingNodes(ClusterState, boolean)}), allowing
 * or disallowing changes to its elements.
 *
 * The main methods used to update routing entries are:
 * <ul>
 * <li> {@link #initializeShard} initializes an unassigned shard.
 * <li> {@link #startShard} starts an initializing shard / completes relocation of a shard.
 * <li> {@link #relocateShard} starts relocation of a started shard.
 * <li> {@link #failShard} fails/cancels an assigned shard.
 * </ul>
 */
public class RoutingNodes implements Iterable<RoutingNode> {

    private final Map<String, RoutingNode> nodesToShards = new HashMap<>();

    private final UnassignedShards unassignedShards = new UnassignedShards(this);

    private final Map<ShardId, List<ShardRouting>> assignedShards = new HashMap<>();

    private final boolean readOnly;

    private int inactivePrimaryCount = 0;

    private int inactiveShardCount = 0;

    private int relocatingShards = 0;

    private final Map<String, Set<String>> attributeValuesByAttribute = new HashMap<>();
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
                            // LinkedHashMap to preserve order.
                            // Add the counterpart shard with relocatingNodeId reflecting the source from which
                            // it's relocating from.
                            entries = nodesToShards.computeIfAbsent(shard.relocatingNodeId(),
                                k -> new LinkedHashMap<>());
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

        if (routing.recoverySource().getType() == RecoverySource.Type.PEER) {
            // add/remove corresponding outgoing recovery on node with primary shard
            if (primary == null) {
                throw new IllegalStateException("shard is peer recovering but primary is unassigned");
            }
            Recoveries.getOrAdd(recoveriesPerNode, primary.currentNodeId()).addOutgoing(howMany);

            if (increment == false && routing.primary() && routing.relocatingNodeId() != null) {
                // primary is done relocating, move non-primary recoveries from old primary to new primary
                int numRecoveringReplicas = 0;
                for (ShardRouting assigned : assignedShards(routing.shardId())) {
                    if (assigned.primary() == false && assigned.initializing() &&
                        assigned.recoverySource().getType() == RecoverySource.Type.PEER) {
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
        if (routing.recoverySource() != null && routing.recoverySource().getType() == RecoverySource.Type.PEER) {
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
        ensureMutable();
        return nodesToShards.values().iterator();
    }

    public UnassignedShards unassigned() {
        return this.unassignedShards;
    }

    public RoutingNode node(String nodeId) {
        return nodesToShards.get(nodeId);
    }

    public Set<String> getAttributeValues(String attributeName) {
        // Only ever accessed on the master service thread so no need for synchronization
        assert MasterService.isMasterUpdateThread() || Thread.currentThread().getName().startsWith("TEST-")
                : Thread.currentThread().getName() + " should be the master service thread";
        return attributeValuesByAttribute.computeIfAbsent(attributeName, ignored -> StreamSupport.stream(this.spliterator(), false)
                .map(r -> r.node().getAttributes().get(attributeName)).filter(Objects::nonNull).collect(Collectors.toSet()));
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
     *
     * Since replicas could possibly be on nodes with a older version of ES than
     * the primary is, this will return replicas on the highest version of ES.
     *
     */
    public ShardRouting activeReplicaWithHighestVersion(ShardId shardId) {
        // It's possible for replicaNodeVersion to be null, when disassociating dead nodes
        // that have been removed, the shards are failed, and part of the shard failing
        // calls this method with an out-of-date RoutingNodes, where the version might not
        // be accessible. Therefore, we need to protect against the version being null
        // (meaning the node will be going away).
        return assignedShards(shardId).stream()
                .filter(shr -> shr.primary() == false && shr.active())
                .filter(shr -> node(shr.currentNodeId()) != null)
                .max(Comparator.comparing(shr -> node(shr.currentNodeId()).node(),
                                Comparator.nullsFirst(Comparator.comparing(DiscoveryNode::getVersion))))
                .orElse(null);
    }

    /**
     * Returns <code>true</code> iff all replicas are active for the given shard routing. Otherwise <code>false</code>
     */
    public boolean allReplicasActive(ShardId shardId, Metadata metadata) {
        final List<ShardRouting> shards = assignedShards(shardId);
        if (shards.isEmpty() || shards.size() < metadata.getIndexSafe(shardId.getIndex()).getNumberOfReplicas() + 1) {
            return false; // if we are empty nothing is active if we have less than total at least one is unassigned
        }
        for (ShardRouting shard : shards) {
            if (shard.active() == false) {
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

    @Override
    public String toString() {
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
    public ShardRouting initializeShard(ShardRouting unassignedShard, String nodeId, @Nullable String existingAllocationId,
                                        long expectedSize, RoutingChangesObserver routingChangesObserver) {
        ensureMutable();
        assert unassignedShard.unassigned() : "expected an unassigned shard " + unassignedShard;
        ShardRouting initializedShard = unassignedShard.initialize(nodeId, existingAllocationId, expectedSize);
        node(nodeId).add(initializedShard);
        inactiveShardCount++;
        if (initializedShard.primary()) {
            inactivePrimaryCount++;
        }
        addRecovery(initializedShard);
        assignedShardsAdd(initializedShard);
        routingChangesObserver.shardInitialized(unassignedShard, initializedShard);
        return initializedShard;
    }

    /**
     * Relocate a shard to another node, adding the target initializing
     * shard as well as assigning it.
     *
     * @return pair of source relocating and target initializing shards.
     */
    public Tuple<ShardRouting,ShardRouting> relocateShard(ShardRouting startedShard, String nodeId, long expectedShardSize,
                                                          RoutingChangesObserver changes) {
        ensureMutable();
        relocatingShards++;
        ShardRouting source = startedShard.relocate(nodeId, expectedShardSize);
        ShardRouting target = source.getTargetRelocatingShard();
        updateAssigned(startedShard, source);
        node(target.currentNodeId()).add(target);
        assignedShardsAdd(target);
        addRecovery(target);
        changes.relocationStarted(startedShard, target);
        return Tuple.tuple(source, target);
    }

    /**
     * Applies the relevant logic to start an initializing shard.
     *
     * Moves the initializing shard to started. If the shard is a relocation target, also removes the relocation source.
     *
     * If the started shard is a primary relocation target, this also reinitializes currently initializing replicas as their
     * recovery source changes
     *
     * @return the started shard
     */
    public ShardRouting startShard(Logger logger, ShardRouting initializingShard, RoutingChangesObserver routingChangesObserver) {
        ensureMutable();
        ShardRouting startedShard = started(initializingShard);
        logger.trace("{} marked shard as started (routing: {})", initializingShard.shardId(), initializingShard);
        routingChangesObserver.shardStarted(initializingShard, startedShard);

        if (initializingShard.relocatingNodeId() != null) {
            // relocation target has been started, remove relocation source
            RoutingNode relocationSourceNode = node(initializingShard.relocatingNodeId());
            ShardRouting relocationSourceShard = relocationSourceNode.getByShardId(initializingShard.shardId());
            assert relocationSourceShard.isRelocationSourceOf(initializingShard);
            assert relocationSourceShard.getTargetRelocatingShard() == initializingShard : "relocation target mismatch, expected: "
                + initializingShard + " but was: " + relocationSourceShard.getTargetRelocatingShard();
            remove(relocationSourceShard);
            routingChangesObserver.relocationCompleted(relocationSourceShard);

            // if this is a primary shard with ongoing replica recoveries, reinitialize them as their recovery source changed
            if (startedShard.primary()) {
                List<ShardRouting> assignedShards = assignedShards(startedShard.shardId());
                // copy list to prevent ConcurrentModificationException
                for (ShardRouting routing : new ArrayList<>(assignedShards)) {
                    if (routing.initializing() && routing.primary() == false) {
                        if (routing.isRelocationTarget()) {
                            // find the relocation source
                            ShardRouting sourceShard = getByAllocationId(routing.shardId(), routing.allocationId().getRelocationId());
                            // cancel relocation and start relocation to same node again
                            ShardRouting startedReplica = cancelRelocation(sourceShard);
                            remove(routing);
                            routingChangesObserver.shardFailed(routing,
                                new UnassignedInfo(UnassignedInfo.Reason.REINITIALIZED, "primary changed"));
                            relocateShard(startedReplica, sourceShard.relocatingNodeId(),
                                sourceShard.getExpectedShardSize(), routingChangesObserver);
                        } else {
                            ShardRouting reinitializedReplica = reinitReplica(routing);
                            routingChangesObserver.initializedReplicaReinitialized(routing, reinitializedReplica);
                        }
                    }
                }
            }
        }
        return startedShard;
    }

    /**
     * Applies the relevant logic to handle a cancelled or failed shard.
     *
     * Moves the shard to unassigned or completely removes the shard (if relocation target).
     *
     * - If shard is a primary, this also fails initializing replicas.
     * - If shard is an active primary, this also promotes an active replica to primary (if such a replica exists).
     * - If shard is a relocating primary, this also removes the primary relocation target shard.
     * - If shard is a relocating replica, this promotes the replica relocation target to a full initializing replica, removing the
     *   relocation source information. This is possible as peer recovery is always done from the primary.
     * - If shard is a (primary or replica) relocation target, this also clears the relocation information on the source shard.
     *
     */
    public void failShard(Logger logger, ShardRouting failedShard, UnassignedInfo unassignedInfo, IndexMetadata indexMetadata,
                          RoutingChangesObserver routingChangesObserver) {
        ensureMutable();
        assert failedShard.assignedToNode() : "only assigned shards can be failed";
        assert indexMetadata.getIndex().equals(failedShard.index()) :
            "shard failed for unknown index (shard entry: " + failedShard + ")";
        assert getByAllocationId(failedShard.shardId(), failedShard.allocationId().getId()) == failedShard :
            "shard routing to fail does not exist in routing table, expected: " + failedShard + " but was: " +
                getByAllocationId(failedShard.shardId(), failedShard.allocationId().getId());

        logger.debug("{} failing shard {} with unassigned info ({})", failedShard.shardId(), failedShard, unassignedInfo.shortSummary());

        // if this is a primary, fail initializing replicas first (otherwise we move RoutingNodes into an inconsistent state)
        if (failedShard.primary()) {
            List<ShardRouting> assignedShards = assignedShards(failedShard.shardId());
            if (assignedShards.isEmpty() == false) {
                // copy list to prevent ConcurrentModificationException
                for (ShardRouting routing : new ArrayList<>(assignedShards)) {
                    if (routing.primary() == false && routing.initializing()) {
                        // re-resolve replica as earlier iteration could have changed source/target of replica relocation
                        ShardRouting replicaShard = getByAllocationId(routing.shardId(), routing.allocationId().getId());
                        assert replicaShard != null : "failed to re-resolve " + routing + " when failing replicas";
                        UnassignedInfo primaryFailedUnassignedInfo = new UnassignedInfo(
                            UnassignedInfo.Reason.PRIMARY_FAILED,
                            "primary failed while replica initializing",
                            null,
                            0,
                            unassignedInfo.getUnassignedTimeInNanos(),
                            unassignedInfo.getUnassignedTimeInMillis(),
                            false,
                            AllocationStatus.NO_ATTEMPT,
                            Collections.emptySet(),
                            routing.currentNodeId()
                        );
                        failShard(logger, replicaShard, primaryFailedUnassignedInfo, indexMetadata, routingChangesObserver);
                    }
                }
            }
        }

        if (failedShard.relocating()) {
            // find the shard that is initializing on the target node
            ShardRouting targetShard = getByAllocationId(failedShard.shardId(), failedShard.allocationId().getRelocationId());
            assert targetShard.isRelocationTargetOf(failedShard);
            if (failedShard.primary()) {
                logger.trace("{} is removed due to the failure/cancellation of the source shard", targetShard);
                // cancel and remove target shard
                remove(targetShard);
                routingChangesObserver.shardFailed(targetShard, unassignedInfo);
            } else {
                logger.trace("{}, relocation source failed / cancelled, mark as initializing without relocation source", targetShard);
                // promote to initializing shard without relocation source and ensure that removed relocation source
                // is not added back as unassigned shard
                removeRelocationSource(targetShard);
                routingChangesObserver.relocationSourceRemoved(targetShard);
            }
        }

        // fail actual shard
        if (failedShard.initializing()) {
            if (failedShard.relocatingNodeId() == null) {
                if (failedShard.primary()) {
                    // promote active replica to primary if active replica exists (only the case for shadow replicas)
                    unassignPrimaryAndPromoteActiveReplicaIfExists(failedShard, unassignedInfo, routingChangesObserver);
                } else {
                    // initializing shard that is not relocation target, just move to unassigned
                    moveToUnassigned(failedShard, unassignedInfo);
                }
            } else {
                // The shard is a target of a relocating shard. In that case we only need to remove the target shard and cancel the source
                // relocation. No shard is left unassigned
                logger.trace("{} is a relocation target, resolving source to cancel relocation ({})", failedShard,
                    unassignedInfo.shortSummary());
                ShardRouting sourceShard = getByAllocationId(failedShard.shardId(),
                    failedShard.allocationId().getRelocationId());
                assert sourceShard.isRelocationSourceOf(failedShard);
                logger.trace("{}, resolved source to [{}]. canceling relocation ... ({})", failedShard.shardId(), sourceShard,
                    unassignedInfo.shortSummary());
                cancelRelocation(sourceShard);
                remove(failedShard);
            }
        } else {
            assert failedShard.active();
            if (failedShard.primary()) {
                // promote active replica to primary if active replica exists
                unassignPrimaryAndPromoteActiveReplicaIfExists(failedShard, unassignedInfo, routingChangesObserver);
            } else {
                if (failedShard.relocating()) {
                    remove(failedShard);
                } else {
                    moveToUnassigned(failedShard, unassignedInfo);
                }
            }
        }
        routingChangesObserver.shardFailed(failedShard, unassignedInfo);
        assert node(failedShard.currentNodeId()).getByShardId(failedShard.shardId()) == null : "failedShard " + failedShard +
            " was matched but wasn't removed";
    }

    private void unassignPrimaryAndPromoteActiveReplicaIfExists(ShardRouting failedShard, UnassignedInfo unassignedInfo,
                                                                RoutingChangesObserver routingChangesObserver) {
        assert failedShard.primary();
        ShardRouting activeReplica = activeReplicaWithHighestVersion(failedShard.shardId());
        if (activeReplica == null) {
            moveToUnassigned(failedShard, unassignedInfo);
        } else {
            movePrimaryToUnassignedAndDemoteToReplica(failedShard, unassignedInfo);
            promoteReplicaToPrimary(activeReplica, routingChangesObserver);
        }
    }

    private void promoteReplicaToPrimary(ShardRouting activeReplica, RoutingChangesObserver routingChangesObserver) {
        // if the activeReplica was relocating before this call to failShard, its relocation was cancelled earlier when we
        // failed initializing replica shards (and moved replica relocation source back to started)
        assert activeReplica.started() : "replica relocation should have been cancelled: " + activeReplica;
        promoteActiveReplicaShardToPrimary(activeReplica);
        routingChangesObserver.replicaPromoted(activeReplica);
    }

    /**
     * Mark a shard as started and adjusts internal statistics.
     *
     * @return the started shard
     */
    private ShardRouting started(ShardRouting shard) {
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
    private ShardRouting cancelRelocation(ShardRouting shard) {
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
    private ShardRouting promoteActiveReplicaShardToPrimary(ShardRouting replicaShard) {
        assert replicaShard.active() : "non-active shard cannot be promoted to primary: " + replicaShard;
        assert replicaShard.primary() == false : "primary shard cannot be promoted to primary: " + replicaShard;
        ShardRouting primaryShard = replicaShard.moveActiveReplicaToPrimary();
        updateAssigned(replicaShard, primaryShard);
        return primaryShard;
    }

    private static final List<ShardRouting> EMPTY = Collections.emptyList();

    /**
     * Cancels the give shard from the Routing nodes internal statistics and cancels
     * the relocation if the shard is relocating.
     */
    private void remove(ShardRouting shard) {
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
    private ShardRouting removeRelocationSource(ShardRouting shard) {
        assert shard.isRelocationTarget() : "only relocation target shards can have their relocation source removed (" + shard + ")";
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

    private ShardRouting reinitReplica(ShardRouting shard) {
        assert shard.primary() == false : "shard must be a replica: " + shard;
        assert shard.initializing() : "can only reinitialize an initializing replica: " + shard;
        assert shard.isRelocationTarget() == false : "replication target cannot be reinitialized: " + shard;
        ShardRouting reinitializedShard = shard.reinitializeReplicaShard();
        updateAssigned(shard, reinitializedShard);
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

    private ShardRouting moveToUnassigned(ShardRouting shard, UnassignedInfo unassignedInfo) {
        assert shard.unassigned() == false : "only assigned shards can be moved to unassigned (" + shard + ")";
        remove(shard);
        ShardRouting unassigned = shard.moveToUnassigned(unassignedInfo);
        unassignedShards.add(unassigned);
        return unassigned;
    }

    /**
     * Moves assigned primary to unassigned and demotes it to a replica.
     * Used in conjunction with {@link #promoteActiveReplicaShardToPrimary} when an active replica is promoted to primary.
     */
    private ShardRouting movePrimaryToUnassignedAndDemoteToReplica(ShardRouting shard, UnassignedInfo unassignedInfo) {
        assert shard.unassigned() == false : "only assigned shards can be moved to unassigned (" + shard + ")";
        assert shard.primary() : "only primary can be demoted to replica (" + shard + ")";
        remove(shard);
        ShardRouting unassigned = shard.moveToUnassigned(unassignedInfo).moveUnassignedFromPrimary();
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
            nodes.ensureMutable();
            CollectionUtil.timSort(unassigned, comparator);
        }

        /**
         * Returns the size of the non-ignored unassigned shards
         */
        public int size() { return unassigned.size(); }

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
         * @see UnassignedIterator#removeAndIgnore(AllocationStatus, RoutingChangesObserver)
         * @see #isIgnoredEmpty()
         */
        public void ignoreShard(ShardRouting shard, AllocationStatus allocationStatus, RoutingChangesObserver changes) {
            nodes.ensureMutable();
            if (shard.primary()) {
                ignoredPrimaries++;
                UnassignedInfo currInfo = shard.unassignedInfo();
                assert currInfo != null;
                if (allocationStatus.equals(currInfo.getLastAllocationStatus()) == false) {
                    UnassignedInfo newInfo = new UnassignedInfo(
                        currInfo.getReason(),
                        currInfo.getMessage(),
                        currInfo.getFailure(),
                        currInfo.getNumFailedAllocations(),
                        currInfo.getUnassignedTimeInNanos(),
                        currInfo.getUnassignedTimeInMillis(),
                        currInfo.isDelayed(),
                        allocationStatus,
                        currInfo.getFailedNodeIds(),
                        currInfo.getLastAllocatedNodeId()
                    );
                    ShardRouting updatedShard = shard.updateUnassigned(newInfo, shard.recoverySource());
                    changes.unassignedInfoUpdated(shard, newInfo);
                    shard = updatedShard;
                }
            }
            ignored.add(shard);
        }

        public class UnassignedIterator implements Iterator<ShardRouting>, ExistingShardsAllocator.UnassignedAllocationHandler {

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
            @Override
            public ShardRouting initialize(String nodeId, @Nullable String existingAllocationId, long expectedShardSize,
                                           RoutingChangesObserver routingChangesObserver) {
                nodes.ensureMutable();
                innerRemove();
                return nodes.initializeShard(current, nodeId, existingAllocationId, expectedShardSize, routingChangesObserver);
            }

            /**
             * Removes and ignores the unassigned shard (will be ignored for this run, but
             * will be added back to unassigned once the metadata is constructed again).
             * Typically this is used when an allocation decision prevents a shard from being allocated such
             * that subsequent consumers of this API won't try to allocate this shard again.
             *
             * @param attempt the result of the allocation attempt
             */
            @Override
            public void removeAndIgnore(AllocationStatus attempt, RoutingChangesObserver changes) {
                nodes.ensureMutable();
                innerRemove();
                ignoreShard(current, attempt, changes);
            }

            private void updateShardRouting(ShardRouting shardRouting) {
                current = shardRouting;
                iterator.set(shardRouting);
            }

            /**
             * updates the unassigned info and recovery source on the current unassigned shard
             *
             * @param  unassignedInfo the new unassigned info to use
             * @param  recoverySource the new recovery source to use
             * @return the shard with unassigned info updated
             */
            @Override
            public ShardRouting updateUnassigned(UnassignedInfo unassignedInfo, RecoverySource recoverySource,
                                                 RoutingChangesObserver changes) {
                nodes.ensureMutable();
                ShardRouting updatedShardRouting = current.updateUnassigned(unassignedInfo, recoverySource);
                changes.unassignedInfoUpdated(current, unassignedInfo);
                updateShardRouting(updatedShardRouting);
                return updatedShardRouting;
            }

            /**
             * Unsupported operation, just there for the interface. Use
             * {@link #removeAndIgnore(AllocationStatus, RoutingChangesObserver)} or
             * {@link #initialize(String, String, long, RoutingChangesObserver)}.
             */
            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported in unassigned iterator," +
                    " use removeAndIgnore or initialize");
            }

            private void innerRemove() {
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
         * @see UnassignedShards#ignoreShard(ShardRouting, AllocationStatus, RoutingChangesObserver)
         * @see UnassignedIterator#removeAndIgnore(AllocationStatus, RoutingChangesObserver)
         */
        public boolean isIgnoredEmpty() {
            return ignored.isEmpty();
        }

        public void shuffle() {
            nodes.ensureMutable();
            Randomness.shuffle(unassigned);
        }

        /**
         * Drains all unassigned shards and returns it.
         * This method will not drain ignored shards.
         */
        public ShardRouting[] drain() {
            nodes.ensureMutable();
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
        if (Assertions.ENABLED == false) {
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

        final Map<ShardId, HashSet<ShardRouting>> shardsByShardId = new HashMap<>();
        for (final RoutingNode routingNode: routingNodes) {
            for (final ShardRouting shardRouting : routingNode) {
                final HashSet<ShardRouting> shards =
                        shardsByShardId.computeIfAbsent(new ShardId(shardRouting.index(), shardRouting.id()), k -> new HashSet<>());
                shards.add(shardRouting);
            }
        }

        for (final Map.Entry<Index, Integer> e : entries) {
            final Index index = e.getKey();
            for (int i = 0; i <= e.getValue(); i++) {
                final ShardId shardId = new ShardId(index, i);
                final HashSet<ShardRouting> shards = shardsByShardId.get(shardId);
                final List<ShardRouting> mutableShardRoutings = routingNodes.assignedShards(shardId);
                assert (shards == null && mutableShardRoutings.size() == 0)
                        || (shards != null && shards.size() == mutableShardRoutings.size() && shards.containsAll(mutableShardRoutings));
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
                    if (routing.primary() && routing.isRelocationTarget() == false) {
                        for (ShardRouting assigned : routingNodes.assignedShards.get(routing.shardId())) {
                            if (assigned.initializing() && assigned.recoverySource().getType() == RecoverySource.Type.PEER) {
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
                "Unassigned primaries is [" + unassignedPrimaryCount + "] but RoutingNodes returned unassigned primaries [" +
                    routingNodes.unassigned().getNumPrimaries() + "]";
        assert unassignedIgnoredPrimaryCount == routingNodes.unassignedShards.getNumIgnoredPrimaries() :
                "Unassigned ignored primaries is [" + unassignedIgnoredPrimaryCount +
                    "] but RoutingNodes returned unassigned ignored primaries [" + routingNodes.unassigned().getNumIgnoredPrimaries() + "]";
        assert inactivePrimaryCount == routingNodes.inactivePrimaryCount :
                "Inactive Primary count [" + inactivePrimaryCount + "] but RoutingNodes returned inactive primaries [" +
                    routingNodes.inactivePrimaryCount + "]";
        assert inactiveShardCount == routingNodes.inactiveShardCount :
                "Inactive Shard count [" + inactiveShardCount + "] but RoutingNodes returned inactive shards [" +
                    routingNodes.inactiveShardCount + "]";
        assert routingNodes.getRelocatingShardCount() == relocating : "Relocating shards mismatch [" +
            routingNodes.getRelocatingShardCount() + "] but expected [" + relocating + "]";

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
                while (queue.isEmpty() == false) {
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
