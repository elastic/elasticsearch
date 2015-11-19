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

package org.elasticsearch.cluster.routing.allocation;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * This service manages the node allocation of a cluster. For this reason the
 * {@link AllocationService} keeps {@link AllocationDeciders} to choose nodes
 * for shard allocation. This class also manages new nodes joining the cluster
 * and rerouting of shards.
 */
public class AllocationService extends AbstractComponent {

    private final AllocationDeciders allocationDeciders;
    private final ClusterInfoService clusterInfoService;
    private final ShardsAllocators shardsAllocators;

    @Inject
    public AllocationService(Settings settings, AllocationDeciders allocationDeciders, ShardsAllocators shardsAllocators, ClusterInfoService clusterInfoService) {
        super(settings);
        this.allocationDeciders = allocationDeciders;
        this.shardsAllocators = shardsAllocators;
        this.clusterInfoService = clusterInfoService;
    }

    /**
     * Applies the started shards. Note, shards can be called several times within this method.
     * <p>
     * If the same instance of the routing table is returned, then no change has been made.</p>
     */
    public RoutingAllocation.Result applyStartedShards(ClusterState clusterState, List<? extends ShardRouting> startedShards) {
        return applyStartedShards(clusterState, startedShards, true);
    }

    public RoutingAllocation.Result applyStartedShards(ClusterState clusterState, List<? extends ShardRouting> startedShards, boolean withReroute) {
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        StartedRerouteAllocation allocation = new StartedRerouteAllocation(allocationDeciders, routingNodes, clusterState.nodes(), startedShards, clusterInfoService.getClusterInfo());
        boolean changed = applyStartedShards(routingNodes, startedShards);
        if (!changed) {
            return new RoutingAllocation.Result(false, clusterState.routingTable());
        }
        shardsAllocators.applyStartedShards(allocation);
        if (withReroute) {
            reroute(allocation);
        }
        RoutingTable routingTable = new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData());
        RoutingAllocation.Result result = new RoutingAllocation.Result(true, routingTable);

        String startedShardsAsString = firstListElementsToCommaDelimitedString(startedShards, s -> s.shardId().toString());
        logClusterHealthStateChange(
                new ClusterStateHealth(clusterState),
                new ClusterStateHealth(clusterState.metaData(), routingTable),
                "shards started [" + startedShardsAsString + "] ..."
        );
        return result;
    }

    public RoutingAllocation.Result applyFailedShard(ClusterState clusterState, ShardRouting failedShard) {
        return applyFailedShards(clusterState, Collections.singletonList(new FailedRerouteAllocation.FailedShard(failedShard, null, null)));
    }

    /**
     * Applies the failed shards. Note, shards can be called several times within this method.
     * <p>
     * If the same instance of the routing table is returned, then no change has been made.</p>
     */
    public RoutingAllocation.Result applyFailedShards(ClusterState clusterState, List<FailedRerouteAllocation.FailedShard> failedShards) {
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        FailedRerouteAllocation allocation = new FailedRerouteAllocation(allocationDeciders, routingNodes, clusterState.nodes(), failedShards, clusterInfoService.getClusterInfo());
        boolean changed = false;
        for (FailedRerouteAllocation.FailedShard failedShard : failedShards) {
            changed |= applyFailedShard(allocation, failedShard.shard, true, new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, failedShard.message, failedShard.failure));
        }
        if (!changed) {
            return new RoutingAllocation.Result(false, clusterState.routingTable());
        }
        shardsAllocators.applyFailedShards(allocation);
        reroute(allocation);
        RoutingTable routingTable = new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData());
        RoutingAllocation.Result result = new RoutingAllocation.Result(true, routingTable);
        String failedShardsAsString = firstListElementsToCommaDelimitedString(failedShards, s -> s.shard.shardId().toString());
        logClusterHealthStateChange(
                new ClusterStateHealth(clusterState),
                new ClusterStateHealth(clusterState.getMetaData(), routingTable),
                "shards failed [" + failedShardsAsString + "] ..."
        );
        return result;
    }

    /**
     * Internal helper to cap the number of elements in a potentially long list for logging.
     *
     * @param elements  The elements to log. May be any non-null list. Must not be null.
     * @param formatter A function that can convert list elements to a String. Must not be null.
     * @param <T>       The list element type.
     * @return A comma-separated string of the first few elements.
     */
    private <T> String firstListElementsToCommaDelimitedString(List<T> elements, Function<T, String> formatter) {
        final int maxNumberOfElements = 10;
        return elements
                .stream()
                .limit(maxNumberOfElements)
                .map(formatter)
                .collect(Collectors.joining(", "));
    }

    public RoutingAllocation.Result reroute(ClusterState clusterState, AllocationCommands commands) {
        return reroute(clusterState, commands, false);
    }

    public RoutingAllocation.Result reroute(ClusterState clusterState, AllocationCommands commands, boolean explain) {
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // we don't shuffle the unassigned shards here, to try and get as close as possible to
        // a consistent result of the effect the commands have on the routing
        // this allows systems to dry run the commands, see the resulting cluster state, and act on it
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState.nodes(), clusterInfoService.getClusterInfo());
        // don't short circuit deciders, we want a full explanation
        allocation.debugDecision(true);
        // we ignore disable allocation, because commands are explicit
        allocation.ignoreDisable(true);
        RoutingExplanations explanations = commands.execute(allocation, explain);
        // we revert the ignore disable flag, since when rerouting, we want the original setting to take place
        allocation.ignoreDisable(false);
        // the assumption is that commands will move / act on shards (or fail through exceptions)
        // so, there will always be shard "movements", so no need to check on reroute
        reroute(allocation);
        RoutingTable routingTable = new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData());
        RoutingAllocation.Result result = new RoutingAllocation.Result(true, routingTable, explanations);
        logClusterHealthStateChange(
                new ClusterStateHealth(clusterState),
                new ClusterStateHealth(clusterState.getMetaData(), routingTable),
                "reroute commands"
        );
        return result;
    }

    /**
     * Reroutes the routing table based on the live nodes.
     * <p>
     * If the same instance of the routing table is returned, then no change has been made.
     */
    public RoutingAllocation.Result reroute(ClusterState clusterState, String reason) {
        return reroute(clusterState, reason, false);
    }

    /**
     * Reroutes the routing table based on the live nodes.
     * <p>
     * If the same instance of the routing table is returned, then no change has been made.
     */
    protected RoutingAllocation.Result reroute(ClusterState clusterState, String reason, boolean debug) {
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState.nodes(), clusterInfoService.getClusterInfo());
        allocation.debugDecision(debug);
        if (!reroute(allocation)) {
            return new RoutingAllocation.Result(false, clusterState.routingTable());
        }
        RoutingTable routingTable = new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(clusterState.metaData());
        RoutingAllocation.Result result = new RoutingAllocation.Result(true, routingTable);
        logClusterHealthStateChange(
                new ClusterStateHealth(clusterState),
                new ClusterStateHealth(clusterState.getMetaData(), routingTable),
                reason
        );
        return result;
    }

    private void logClusterHealthStateChange(ClusterStateHealth previousStateHealth, ClusterStateHealth newStateHealth, String reason) {
        ClusterHealthStatus previousHealth = previousStateHealth.getStatus();
        ClusterHealthStatus currentHealth = newStateHealth.getStatus();
        if (!previousHealth.equals(currentHealth)) {
            logger.info("Cluster health status changed from [{}] to [{}] (reason: [{}]).", previousHealth, currentHealth, reason);
        }
    }

    private boolean reroute(RoutingAllocation allocation) {
        boolean changed = false;
        // first, clear from the shards any node id they used to belong to that is now dead
        changed |= deassociateDeadNodes(allocation);

        // create a sorted list of from nodes with least number of shards to the maximum ones
        applyNewNodes(allocation);

        // elect primaries *before* allocating unassigned, so backups of primaries that failed
        // will be moved to primary state and not wait for primaries to be allocated and recovered (*from gateway*)
        changed |= electPrimariesAndUnassignedDanglingReplicas(allocation);

        // now allocate all the unassigned to available nodes
        if (allocation.routingNodes().unassigned().size() > 0) {
            changed |= shardsAllocators.allocateUnassigned(allocation);
        }

        // move shards that no longer can be allocated
        changed |= moveShards(allocation);

        // rebalance
        changed |= shardsAllocators.rebalance(allocation);
        assert RoutingNodes.assertShardStats(allocation.routingNodes());
        return changed;
    }

    private boolean moveShards(RoutingAllocation allocation) {
        boolean changed = false;

        // create a copy of the shards interleaving between nodes, and check if they can remain
        List<ShardRouting> shards = new ArrayList<>();
        int index = 0;
        boolean found = true;
        final RoutingNodes routingNodes = allocation.routingNodes();
        while (found) {
            found = false;
            for (RoutingNode routingNode : routingNodes) {
                if (index >= routingNode.size()) {
                    continue;
                }
                found = true;
                shards.add(routingNode.get(index));
            }
            index++;
        }
        for (int i = 0; i < shards.size(); i++) {
            ShardRouting shardRouting = shards.get(i);
            // we can only move started shards...
            if (!shardRouting.started()) {
                continue;
            }
            final RoutingNode routingNode = routingNodes.node(shardRouting.currentNodeId());
            Decision decision = allocation.deciders().canRemain(shardRouting, routingNode, allocation);
            if (decision.type() == Decision.Type.NO) {
                logger.debug("[{}][{}] allocated on [{}], but can no longer be allocated on it, moving...", shardRouting.index(), shardRouting.id(), routingNode.node());
                boolean moved = shardsAllocators.move(shardRouting, routingNode, allocation);
                if (!moved) {
                    logger.debug("[{}][{}] can't move", shardRouting.index(), shardRouting.id());
                } else {
                    changed = true;
                }
            }
        }
        return changed;
    }

    private boolean electPrimariesAndUnassignedDanglingReplicas(RoutingAllocation allocation) {
        boolean changed = false;
        RoutingNodes routingNodes = allocation.routingNodes();
        if (routingNodes.unassigned().getNumPrimaries() == 0) {
            // move out if we don't have unassigned primaries
            return changed;
        }

        // go over and remove dangling replicas that are initializing for primary shards
        List<ShardRouting> shardsToFail = new ArrayList<>();
        for (ShardRouting shardEntry : routingNodes.unassigned()) {
            if (shardEntry.primary()) {
                for (ShardRouting routing : routingNodes.assignedShards(shardEntry)) {
                    if (!routing.primary() && routing.initializing()) {
                        shardsToFail.add(routing);
                    }
                }

            }
        }
        for (ShardRouting shardToFail : shardsToFail) {
            changed |= applyFailedShard(allocation, shardToFail, false, new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "primary failed while replica initializing"));
        }

        // now, go over and elect a new primary if possible, not, from this code block on, if one is elected,
        // routingNodes.hasUnassignedPrimaries() will potentially be false

        for (ShardRouting shardEntry : routingNodes.unassigned()) {
            if (shardEntry.primary()) {
                ShardRouting candidate = allocation.routingNodes().activeReplica(shardEntry);
                if (candidate != null) {
                    IndexMetaData index = allocation.metaData().index(candidate.index());
                    routingNodes.swapPrimaryFlag(shardEntry, candidate);
                    if (candidate.relocatingNodeId() != null) {
                        changed = true;
                        // its also relocating, make sure to move the other routing to primary
                        RoutingNode node = routingNodes.node(candidate.relocatingNodeId());
                        if (node != null) {
                            for (ShardRouting shardRouting : node) {
                                if (shardRouting.shardId().equals(candidate.shardId()) && !shardRouting.primary()) {
                                    routingNodes.swapPrimaryFlag(shardRouting);
                                    break;
                                }
                            }
                        }
                    }
                    if (IndexMetaData.isIndexUsingShadowReplicas(index.getSettings())) {
                        routingNodes.reinitShadowPrimary(candidate);
                        changed = true;
                    }
                }
            }
        }

        return changed;
    }

    /**
     * Applies the new nodes to the routing nodes and returns them (just the
     * new nodes);
     */
    private void applyNewNodes(RoutingAllocation allocation) {
        final RoutingNodes routingNodes = allocation.routingNodes();
        for (ObjectCursor<DiscoveryNode> cursor : allocation.nodes().dataNodes().values()) {
            DiscoveryNode node = cursor.value;
            if (!routingNodes.isKnown(node)) {
                routingNodes.addNode(node);
            }
        }
    }

    private boolean deassociateDeadNodes(RoutingAllocation allocation) {
        boolean changed = false;
        for (RoutingNodes.RoutingNodesIterator it = allocation.routingNodes().nodes(); it.hasNext(); ) {
            RoutingNode node = it.next();
            if (allocation.nodes().dataNodes().containsKey(node.nodeId())) {
                // its a live node, continue
                continue;
            }
            changed = true;
            // now, go over all the shards routing on the node, and fail them
            UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, "node_left[" + node.nodeId() + "]");
            for (ShardRouting shardRouting : node.copyShards()) {
                applyFailedShard(allocation, shardRouting, false, unassignedInfo);
            }
            // its a dead node, remove it, note, its important to remove it *after* we apply failed shard
            // since it relies on the fact that the RoutingNode exists in the list of nodes
            it.remove();
        }
        return changed;
    }

    private boolean applyStartedShards(RoutingNodes routingNodes, Iterable<? extends ShardRouting> startedShardEntries) {
        boolean dirty = false;
        // apply shards might be called several times with the same shard, ignore it
        for (ShardRouting startedShard : startedShardEntries) {
            assert startedShard.initializing();

            // validate index still exists. strictly speaking this is not needed but it gives clearer logs
            if (routingNodes.routingTable().index(startedShard.index()) == null) {
                logger.debug("{} ignoring shard started, unknown index (routing: {})", startedShard.shardId(), startedShard);
                continue;
            }


            RoutingNodes.RoutingNodeIterator currentRoutingNode = routingNodes.routingNodeIter(startedShard.currentNodeId());
            if (currentRoutingNode == null) {
                logger.debug("{} failed to find shard in order to start it [failed to find node], ignoring (routing: {})", startedShard.shardId(), startedShard);
                continue;
            }

            for (ShardRouting shard : currentRoutingNode) {
                if (shard.isSameAllocation(startedShard)) {
                    if (shard.active()) {
                        logger.trace("{} shard is already started, ignoring (routing: {})", startedShard.shardId(), startedShard);
                    } else {
                        dirty = true;
                        // override started shard with the latest copy. Capture it now , before starting the shard destroys it...
                        startedShard = new ShardRouting(shard);
                        routingNodes.started(shard);
                        logger.trace("{} marked shard as started (routing: {})", startedShard.shardId(), startedShard);
                    }
                    break;
                }
            }

            // startedShard is the current state of the shard (post relocation for example)
            // this means that after relocation, the state will be started and the currentNodeId will be
            // the node we relocated to
            if (startedShard.relocatingNodeId() == null) {
                continue;
            }

            RoutingNodes.RoutingNodeIterator sourceRoutingNode = routingNodes.routingNodeIter(startedShard.relocatingNodeId());
            if (sourceRoutingNode != null) {
                while (sourceRoutingNode.hasNext()) {
                    ShardRouting shard = sourceRoutingNode.next();
                    if (shard.isRelocationSourceOf(startedShard)) {
                        dirty = true;
                        sourceRoutingNode.remove();
                        break;
                    }
                }
            }
        }
        return dirty;
    }

    /**
     * Applies the relevant logic to handle a failed shard. Returns <tt>true</tt> if changes happened that
     * require relocation.
     */
    private boolean applyFailedShard(RoutingAllocation allocation, ShardRouting failedShard, boolean addToIgnoreList, UnassignedInfo unassignedInfo) {
        IndexRoutingTable indexRoutingTable = allocation.routingTable().index(failedShard.index());
        if (indexRoutingTable == null) {
            logger.debug("{} ignoring shard failure, unknown index in {} ({})", failedShard.shardId(), failedShard, unassignedInfo.shortSummary());
            return false;
        }

        RoutingNodes routingNodes = allocation.routingNodes();

        RoutingNodes.RoutingNodeIterator matchedNode = routingNodes.routingNodeIter(failedShard.currentNodeId());
        if (matchedNode == null) {
            logger.debug("{} ignoring shard failure, unknown node in {} ({})", failedShard.shardId(), failedShard, unassignedInfo.shortSummary());
            return false;
        }

        boolean matchedShard = false;
        while (matchedNode.hasNext()) {
            ShardRouting routing = matchedNode.next();
            if (routing.isSameAllocation(failedShard)) {
                matchedShard = true;
                logger.debug("{} failed shard {} found in routingNodes, failing it ({})", failedShard.shardId(), failedShard, unassignedInfo.shortSummary());
                break;
            }
        }

        if (matchedShard == false) {
            logger.debug("{} ignoring shard failure, unknown allocation id in {} ({})", failedShard.shardId(), failedShard, unassignedInfo.shortSummary());
            return false;
        }

        // replace incoming instance to make sure we work on the latest one. Copy it to maintain information during modifications.
        failedShard = new ShardRouting(matchedNode.current());

        // remove the current copy of the shard
        matchedNode.remove();

        if (addToIgnoreList) {
            // make sure we ignore this shard on the relevant node
            allocation.addIgnoreShardForNode(failedShard.shardId(), failedShard.currentNodeId());
        }

        if (failedShard.relocatingNodeId() != null && failedShard.initializing()) {
            // The shard is a target of a relocating shard. In that case we only
            // need to remove the target shard and cancel the source relocation.
            // No shard is left unassigned
            logger.trace("{} is a relocation target, resolving source to cancel relocation ({})", failedShard, unassignedInfo.shortSummary());
            RoutingNode relocatingFromNode = routingNodes.node(failedShard.relocatingNodeId());
            if (relocatingFromNode != null) {
                for (ShardRouting shardRouting : relocatingFromNode) {
                    if (shardRouting.isRelocationSourceOf(failedShard)) {
                        logger.trace("{}, resolved source to [{}]. canceling relocation ... ({})", failedShard.shardId(), shardRouting, unassignedInfo.shortSummary());
                        routingNodes.cancelRelocation(shardRouting);
                        break;
                    }
                }
            }
        } else {
            // The fail shard is the main copy of the current shard routing. Any
            // relocation will be cancelled (and the target shard removed as well)
            // and the shard copy needs to be marked as unassigned

            if (failedShard.relocatingNodeId() != null) {
                // handle relocation source shards.  we need to find the target initializing shard that is recovering, and remove it...
                assert failedShard.initializing() == false; // should have been dealt with and returned
                assert failedShard.relocating();

                RoutingNodes.RoutingNodeIterator initializingNode = routingNodes.routingNodeIter(failedShard.relocatingNodeId());
                if (initializingNode != null) {
                    while (initializingNode.hasNext()) {
                        ShardRouting shardRouting = initializingNode.next();
                        if (shardRouting.isRelocationTargetOf(failedShard)) {
                            logger.trace("{} is removed due to the failure of the source shard", shardRouting);
                            initializingNode.remove();
                        }
                    }
                }
            }

            matchedNode.moveToUnassigned(unassignedInfo);
        }
        assert matchedNode.isRemoved() : "failedShard " + failedShard + " was matched but wasn't removed";
        return true;
    }

    private RoutingNodes getMutableRoutingNodes(ClusterState clusterState) {
        RoutingNodes routingNodes = new RoutingNodes(clusterState, false); // this is a costly operation - only call this once!
        return routingNodes;
    }
}
