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

package org.elasticsearch.gateway;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.index.shard.ShardStateMetaData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The primary shard allocator allocates primary shard that were not created as
 * a result of an API to a node that held them last to be recovered.
 */
public abstract class PrimaryShardAllocator extends AbstractComponent {

    private static final Function<String, String> INITIAL_SHARDS_PARSER = (value) -> {
        switch (value) {
            case "quorum":
            case "quorum-1":
            case "half":
            case "one":
            case "full":
            case "full-1":
            case "all-1":
            case "all":
                return value;
            default:
                Integer.parseInt(value); // it can be parsed that's all we care here?
                return value;
        }
    };

    public static final Setting<String> NODE_INITIAL_SHARDS_SETTING =
        new Setting<>("gateway.initial_shards", (settings) -> settings.get("gateway.local.initial_shards", "quorum"), INITIAL_SHARDS_PARSER,
            Property.Dynamic, Property.NodeScope);
    @Deprecated
    public static final Setting<String> INDEX_RECOVERY_INITIAL_SHARDS_SETTING =
        new Setting<>("index.recovery.initial_shards", (settings) -> NODE_INITIAL_SHARDS_SETTING.get(settings) , INITIAL_SHARDS_PARSER,
            Property.Dynamic, Property.IndexScope);

    public PrimaryShardAllocator(Settings settings) {
        super(settings);
        logger.debug("using initial_shards [{}]", NODE_INITIAL_SHARDS_SETTING.get(settings));
    }

    public boolean allocateUnassigned(RoutingAllocation allocation) {
        boolean changed = false;
        final RoutingNodes routingNodes = allocation.routingNodes();
        final MetaData metaData = allocation.metaData();

        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            final ShardRouting shard = unassignedIterator.next();

            if (shard.primary() == false) {
                continue;
            }

            final IndexMetaData indexMetaData = metaData.getIndexSafe(shard.index());
            // don't go wild here and create a new IndexSetting object for every shard this could cause a lot of garbage
            // on cluster restart if we allocate a boat load of shards
            if (shard.allocatedPostIndexCreate(indexMetaData) == false) {
                // when we create a fresh index
                continue;
            }

            final AsyncShardFetch.FetchResult<NodeGatewayStartedShards> shardState = fetchData(shard, allocation);
            if (shardState.hasData() == false) {
                logger.trace("{}: ignoring allocation, still fetching shard started state", shard);
                allocation.setHasPendingAsyncFetch();
                unassignedIterator.removeAndIgnore();
                continue;
            }

            final Set<String> lastActiveAllocationIds = indexMetaData.activeAllocationIds(shard.id());
            final boolean snapshotRestore = shard.restoreSource() != null;
            final boolean recoverOnAnyNode = recoverOnAnyNode(indexMetaData);

            final NodeShardsResult nodeShardsResult;
            final boolean enoughAllocationsFound;

            if (lastActiveAllocationIds.isEmpty()) {
                assert Version.indexCreated(indexMetaData.getSettings()).before(Version.V_5_0_0_alpha1) : "trying to allocated a primary with an empty allocation id set, but index is new";
                // when we load an old index (after upgrading cluster) or restore a snapshot of an old index
                // fall back to old version-based allocation mode
                // Note that once the shard has been active, lastActiveAllocationIds will be non-empty
                nodeShardsResult = buildVersionBasedNodeShardsResult(shard, snapshotRestore || recoverOnAnyNode, allocation.getIgnoreNodes(shard.shardId()), shardState);
                if (snapshotRestore || recoverOnAnyNode) {
                    enoughAllocationsFound = nodeShardsResult.allocationsFound > 0;
                } else {
                    enoughAllocationsFound = isEnoughVersionBasedAllocationsFound(indexMetaData, nodeShardsResult);
                }
                logger.debug("[{}][{}]: version-based allocation for pre-{} index found {} allocations of {}", shard.index(), shard.id(), Version.V_5_0_0_alpha1, nodeShardsResult.allocationsFound, shard);
            } else {
                assert lastActiveAllocationIds.isEmpty() == false;
                // use allocation ids to select nodes
                nodeShardsResult = buildAllocationIdBasedNodeShardsResult(shard, snapshotRestore || recoverOnAnyNode,
                        allocation.getIgnoreNodes(shard.shardId()), lastActiveAllocationIds, shardState);
                enoughAllocationsFound = nodeShardsResult.orderedAllocationCandidates.size() > 0;
                logger.debug("[{}][{}]: found {} allocation candidates of {} based on allocation ids: [{}]", shard.index(), shard.id(), nodeShardsResult.orderedAllocationCandidates.size(), shard, lastActiveAllocationIds);
            }

            if (enoughAllocationsFound == false){
                if (snapshotRestore) {
                    // let BalancedShardsAllocator take care of allocating this shard
                    logger.debug("[{}][{}]: missing local data, will restore from [{}]", shard.index(), shard.id(), shard.restoreSource());
                } else if (recoverOnAnyNode) {
                    // let BalancedShardsAllocator take care of allocating this shard
                    logger.debug("[{}][{}]: missing local data, recover from any node", shard.index(), shard.id());
                } else {
                    // we can't really allocate, so ignore it and continue
                    unassignedIterator.removeAndIgnore();
                    logger.debug("[{}][{}]: not allocating, number_of_allocated_shards_found [{}]", shard.index(), shard.id(), nodeShardsResult.allocationsFound);
                }
                continue;
            }

            final NodesToAllocate nodesToAllocate = buildNodesToAllocate(shard, allocation, nodeShardsResult.orderedAllocationCandidates);
            if (nodesToAllocate.yesNodeShards.isEmpty() == false) {
                NodeGatewayStartedShards nodeShardState = nodesToAllocate.yesNodeShards.get(0);
                logger.debug("[{}][{}]: allocating [{}] to [{}] on primary allocation", shard.index(), shard.id(), shard, nodeShardState.getNode());
                changed = true;
                unassignedIterator.initialize(nodeShardState.getNode().getId(), nodeShardState.allocationId(), ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            } else if (nodesToAllocate.throttleNodeShards.isEmpty() == true && nodesToAllocate.noNodeShards.isEmpty() == false) {
                NodeGatewayStartedShards nodeShardState = nodesToAllocate.noNodeShards.get(0);
                logger.debug("[{}][{}]: forcing allocating [{}] to [{}] on primary allocation", shard.index(), shard.id(), shard, nodeShardState.getNode());
                changed = true;
                unassignedIterator.initialize(nodeShardState.getNode().getId(), nodeShardState.allocationId(), ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            } else {
                // we are throttling this, but we have enough to allocate to this node, ignore it for now
                logger.debug("[{}][{}]: throttling allocation [{}] to [{}] on primary allocation", shard.index(), shard.id(), shard, nodesToAllocate.throttleNodeShards);
                unassignedIterator.removeAndIgnore();
            }
        }
        return changed;
    }

    /**
     * Builds a list of nodes. If matchAnyShard is set to false, only nodes that have an allocation id matching
     * lastActiveAllocationIds are added to the list. Otherwise, any node that has a shard is added to the list, but
     * entries with matching allocation id are always at the front of the list.
     */
    protected NodeShardsResult buildAllocationIdBasedNodeShardsResult(ShardRouting shard, boolean matchAnyShard, Set<String> ignoreNodes,
                                                                      Set<String> lastActiveAllocationIds, AsyncShardFetch.FetchResult<NodeGatewayStartedShards> shardState) {
        LinkedList<NodeGatewayStartedShards> matchingNodeShardStates = new LinkedList<>();
        LinkedList<NodeGatewayStartedShards> nonMatchingNodeShardStates = new LinkedList<>();
        int numberOfAllocationsFound = 0;
        for (NodeGatewayStartedShards nodeShardState : shardState.getData().values()) {
            DiscoveryNode node = nodeShardState.getNode();
            String allocationId = nodeShardState.allocationId();

            if (ignoreNodes.contains(node.getId())) {
                continue;
            }

            if (nodeShardState.storeException() == null) {
                if (allocationId == null && nodeShardState.legacyVersion() == ShardStateMetaData.NO_VERSION) {
                    logger.trace("[{}] on node [{}] has no shard state information", shard, nodeShardState.getNode());
                } else if (allocationId != null) {
                    assert nodeShardState.legacyVersion() == ShardStateMetaData.NO_VERSION : "Allocation id and legacy version cannot be both present";
                    logger.trace("[{}] on node [{}] has allocation id [{}]", shard, nodeShardState.getNode(), allocationId);
                } else {
                    logger.trace("[{}] on node [{}] has no allocation id, out-dated shard (shard state version: [{}])", shard, nodeShardState.getNode(), nodeShardState.legacyVersion());
                }
            } else {
                logger.trace("[{}] on node [{}] has allocation id [{}] but the store can not be opened, treating as no allocation id", nodeShardState.storeException(), shard, nodeShardState.getNode(), allocationId);
                allocationId = null;
            }

            if (allocationId != null) {
                numberOfAllocationsFound++;
                if (lastActiveAllocationIds.contains(allocationId)) {
                    if (nodeShardState.primary()) {
                        matchingNodeShardStates.addFirst(nodeShardState);
                    } else {
                        matchingNodeShardStates.addLast(nodeShardState);
                    }
                } else if (matchAnyShard) {
                    if (nodeShardState.primary()) {
                        nonMatchingNodeShardStates.addFirst(nodeShardState);
                    } else {
                        nonMatchingNodeShardStates.addLast(nodeShardState);
                    }
                }
            }
        }

        List<NodeGatewayStartedShards> nodeShardStates = new ArrayList<>();
        nodeShardStates.addAll(matchingNodeShardStates);
        nodeShardStates.addAll(nonMatchingNodeShardStates);

        if (logger.isTraceEnabled()) {
            logger.trace("{} candidates for allocation: {}", shard, nodeShardStates.stream().map(s -> s.getNode().getName()).collect(Collectors.joining(", ")));
        }
        return new NodeShardsResult(nodeShardStates, numberOfAllocationsFound);
    }

    /**
     * used by old version-based allocation
     */
    private boolean isEnoughVersionBasedAllocationsFound(IndexMetaData indexMetaData, NodeShardsResult nodeShardsResult) {
        // check if the counts meets the minimum set
        int requiredAllocation = 1;
        // if we restore from a repository one copy is more then enough
        String initialShards = INDEX_RECOVERY_INITIAL_SHARDS_SETTING.get(indexMetaData.getSettings(), settings);
        if ("quorum".equals(initialShards)) {
            if (indexMetaData.getNumberOfReplicas() > 1) {
                requiredAllocation = ((1 + indexMetaData.getNumberOfReplicas()) / 2) + 1;
            }
        } else if ("quorum-1".equals(initialShards) || "half".equals(initialShards)) {
            if (indexMetaData.getNumberOfReplicas() > 2) {
                requiredAllocation = ((1 + indexMetaData.getNumberOfReplicas()) / 2);
            }
        } else if ("one".equals(initialShards)) {
            requiredAllocation = 1;
        } else if ("full".equals(initialShards) || "all".equals(initialShards)) {
            requiredAllocation = indexMetaData.getNumberOfReplicas() + 1;
        } else if ("full-1".equals(initialShards) || "all-1".equals(initialShards)) {
            if (indexMetaData.getNumberOfReplicas() > 1) {
                requiredAllocation = indexMetaData.getNumberOfReplicas();
            }
        } else {
            requiredAllocation = Integer.parseInt(initialShards);
        }

        return nodeShardsResult.allocationsFound >= requiredAllocation;
    }

    /**
     * Split the list of node shard states into groups yes/no/throttle based on allocation deciders
     */
    private NodesToAllocate buildNodesToAllocate(ShardRouting shard, RoutingAllocation allocation, List<NodeGatewayStartedShards> nodeShardStates) {
        List<NodeGatewayStartedShards> yesNodeShards = new ArrayList<>();
        List<NodeGatewayStartedShards> throttledNodeShards = new ArrayList<>();
        List<NodeGatewayStartedShards> noNodeShards = new ArrayList<>();
        for (NodeGatewayStartedShards nodeShardState : nodeShardStates) {
            RoutingNode node = allocation.routingNodes().node(nodeShardState.getNode().getId());
            if (node == null) {
                continue;
            }

            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            if (decision.type() == Decision.Type.THROTTLE) {
                throttledNodeShards.add(nodeShardState);
            } else if (decision.type() == Decision.Type.NO) {
                noNodeShards.add(nodeShardState);
            } else {
                yesNodeShards.add(nodeShardState);
            }
        }
        return new NodesToAllocate(Collections.unmodifiableList(yesNodeShards), Collections.unmodifiableList(throttledNodeShards), Collections.unmodifiableList(noNodeShards));
    }

    /**
     * Builds a list of previously started shards. If matchAnyShard is set to false, only shards with the highest shard version are added to
     * the list. Otherwise, any existing shard is added to the list, but entries with highest version are always at the front of the list.
     */
    NodeShardsResult buildVersionBasedNodeShardsResult(ShardRouting shard, boolean matchAnyShard, Set<String> ignoreNodes,
                                                       AsyncShardFetch.FetchResult<NodeGatewayStartedShards> shardState) {
        final List<NodeGatewayStartedShards> allocationCandidates = new ArrayList<>();
        int numberOfAllocationsFound = 0;
        long highestVersion = ShardStateMetaData.NO_VERSION;
        for (NodeGatewayStartedShards nodeShardState : shardState.getData().values()) {
            long version = nodeShardState.legacyVersion();
            DiscoveryNode node = nodeShardState.getNode();

            if (ignoreNodes.contains(node.getId())) {
                continue;
            }

            if (nodeShardState.storeException() == null) {
                if (version == ShardStateMetaData.NO_VERSION && nodeShardState.allocationId() == null) {
                    logger.trace("[{}] on node [{}] has no shard state information", shard, nodeShardState.getNode());
                } else if (version != ShardStateMetaData.NO_VERSION) {
                    assert nodeShardState.allocationId() == null : "Allocation id and legacy version cannot be both present";
                    logger.trace("[{}] on node [{}] has version [{}] of shard", shard, nodeShardState.getNode(), version);
                } else {
                    // shard was already selected in a 5.x cluster as primary for recovery, was initialized (and wrote a new state file) but
                    // did not make it to STARTED state before the cluster crashed (otherwise list of active allocation ids would be
                    // non-empty and allocation id - based allocation mode would be chosen).
                    // Prefer this shard copy again.
                    version = Long.MAX_VALUE;
                    logger.trace("[{}] on node [{}] has allocation id [{}]", shard, nodeShardState.getNode(), nodeShardState.allocationId());
                }
            } else {
                // when there is an store exception, we disregard the reported version and assign it as no version (same as shard does not exist)
                logger.trace("[{}] on node [{}] has version [{}] but the store can not be opened, treating no version", nodeShardState.storeException(), shard, nodeShardState.getNode(), version);
                version = ShardStateMetaData.NO_VERSION;
            }

            if (version != ShardStateMetaData.NO_VERSION) {
                numberOfAllocationsFound++;
                // If we've found a new "best" candidate, clear the
                // current candidates and add it
                if (version > highestVersion) {
                    highestVersion = version;
                    if (matchAnyShard == false) {
                        allocationCandidates.clear();
                    }
                    allocationCandidates.add(nodeShardState);
                } else if (version == highestVersion) {
                    // If the candidate is the same, add it to the
                    // list, but keep the current candidate
                    allocationCandidates.add(nodeShardState);
                }
            }
        }
        // sort array so the node with the highest version is at the beginning
        CollectionUtil.timSort(allocationCandidates, Comparator.comparing(NodeGatewayStartedShards::legacyVersion).reversed());

        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("[");
            for (NodeGatewayStartedShards n : allocationCandidates) {
                sb.append("[").append(n.getNode().getName()).append("]").append(" -> ").append(n.legacyVersion()).append(", ");
            }
            sb.append("]");
            logger.trace("{} candidates for allocation: {}", shard, sb.toString());
        }

        return new NodeShardsResult(Collections.unmodifiableList(allocationCandidates), numberOfAllocationsFound);
    }

    /**
     * Return {@code true} if the index is configured to allow shards to be
     * recovered on any node
     */
    private boolean recoverOnAnyNode(IndexMetaData metaData) {
        return (IndexMetaData.isOnSharedFilesystem(metaData.getSettings()) || IndexMetaData.isOnSharedFilesystem(this.settings))
            && IndexMetaData.INDEX_SHARED_FS_ALLOW_RECOVERY_ON_ANY_NODE_SETTING.get(metaData.getSettings(), this.settings);
    }

    protected abstract AsyncShardFetch.FetchResult<NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation);

    static class NodeShardsResult {
        public final List<NodeGatewayStartedShards> orderedAllocationCandidates;
        public final int allocationsFound;

        public NodeShardsResult(List<NodeGatewayStartedShards> orderedAllocationCandidates, int allocationsFound) {
            this.orderedAllocationCandidates = orderedAllocationCandidates;
            this.allocationsFound = allocationsFound;
        }
    }

    static class NodesToAllocate {
        final List<NodeGatewayStartedShards> yesNodeShards;
        final List<NodeGatewayStartedShards> throttleNodeShards;
        final List<NodeGatewayStartedShards> noNodeShards;

        public NodesToAllocate(List<NodeGatewayStartedShards> yesNodeShards, List<NodeGatewayStartedShards> throttleNodeShards,
                               List<NodeGatewayStartedShards> noNodeShards) {
            this.yesNodeShards = yesNodeShards;
            this.throttleNodeShards = throttleNodeShards;
            this.noNodeShards = noNodeShards;
        }
    }
}
