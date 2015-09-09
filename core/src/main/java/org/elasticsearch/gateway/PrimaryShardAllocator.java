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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The primary shard allocator allocates primary shard that were not created as
 * a result of an API to a node that held them last to be recovered.
 */
public abstract class PrimaryShardAllocator extends AbstractComponent {

    public static final String INDEX_RECOVERY_INITIAL_SHARDS = "index.recovery.initial_shards";

    private final String initialShards;

    public PrimaryShardAllocator(Settings settings) {
        super(settings);
        this.initialShards = settings.get("gateway.initial_shards", settings.get("gateway.local.initial_shards", "quorum"));
        logger.debug("using initial_shards [{}]", initialShards);
    }

    public boolean allocateUnassigned(RoutingAllocation allocation) {
        boolean changed = false;
        final RoutingNodes routingNodes = allocation.routingNodes();
        final MetaData metaData = routingNodes.metaData();

        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            ShardRouting shard = unassignedIterator.next();

            if (needToFindPrimaryCopy(shard) == false) {
                continue;
            }

            AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> shardState = fetchData(shard, allocation);
            if (shardState.hasData() == false) {
                logger.trace("{}: ignoring allocation, still fetching shard started state", shard);
                unassignedIterator.removeAndIgnore();
                continue;
            }

            IndexMetaData indexMetaData = metaData.index(shard.getIndex());

            NodesAndVersions nodesAndVersions = buildNodesAndVersions(shard, recoverOnAnyNode(indexMetaData.settings()), allocation.getIgnoreNodes(shard.shardId()), shardState);
            logger.debug("[{}][{}] found {} allocations of {}, highest version: [{}]", shard.index(), shard.id(), nodesAndVersions.allocationsFound, shard, nodesAndVersions.highestVersion);

            if (isEnoughAllocationsFound(shard, indexMetaData, nodesAndVersions) == false) {
                // if we are restoring this shard we still can allocate
                if (shard.restoreSource() == null) {
                    // we can't really allocate, so ignore it and continue
                    unassignedIterator.removeAndIgnore();
                    logger.debug("[{}][{}]: not allocating, number_of_allocated_shards_found [{}]", shard.index(), shard.id(), nodesAndVersions.allocationsFound);
                } else {
                    logger.debug("[{}][{}]: missing local data, will restore from [{}]", shard.index(), shard.id(), shard.restoreSource());
                }
                continue;
            }

            NodesToAllocate nodesToAllocate = buildNodesToAllocate(shard, allocation, nodesAndVersions);
            if (nodesToAllocate.yesNodes.isEmpty() == false) {
                DiscoveryNode node = nodesToAllocate.yesNodes.get(0);
                logger.debug("[{}][{}]: allocating [{}] to [{}] on primary allocation", shard.index(), shard.id(), shard, node);
                changed = true;
                unassignedIterator.initialize(node.id(), nodesAndVersions.highestVersion, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            } else if (nodesToAllocate.throttleNodes.isEmpty() == true && nodesToAllocate.noNodes.isEmpty() == false) {
                DiscoveryNode node = nodesToAllocate.noNodes.get(0);
                logger.debug("[{}][{}]: forcing allocating [{}] to [{}] on primary allocation", shard.index(), shard.id(), shard, node);
                changed = true;
                unassignedIterator.initialize(node.id(), nodesAndVersions.highestVersion, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            } else {
                // we are throttling this, but we have enough to allocate to this node, ignore it for now
                logger.debug("[{}][{}]: throttling allocation [{}] to [{}] on primary allocation", shard.index(), shard.id(), shard, nodesToAllocate.throttleNodes);
                unassignedIterator.removeAndIgnore();
            }
        }
        return changed;
    }

    /**
     * Does the shard need to find a primary copy?
     */
    boolean needToFindPrimaryCopy(ShardRouting shard) {
        if (shard.primary() == false) {
            return false;
        }

        // this is an API allocation, ignore since we know there is no data...
        if (shard.allocatedPostIndexCreate() == false) {
            return false;
        }

        return true;
    }

    private boolean isEnoughAllocationsFound(ShardRouting shard, IndexMetaData indexMetaData, NodesAndVersions nodesAndVersions) {
        // check if the counts meets the minimum set
        int requiredAllocation = 1;
        // if we restore from a repository one copy is more then enough
        if (shard.restoreSource() == null) {
            try {
                String initialShards = indexMetaData.settings().get(INDEX_RECOVERY_INITIAL_SHARDS, settings.get(INDEX_RECOVERY_INITIAL_SHARDS, this.initialShards));
                if ("quorum".equals(initialShards)) {
                    if (indexMetaData.numberOfReplicas() > 1) {
                        requiredAllocation = ((1 + indexMetaData.numberOfReplicas()) / 2) + 1;
                    }
                } else if ("quorum-1".equals(initialShards) || "half".equals(initialShards)) {
                    if (indexMetaData.numberOfReplicas() > 2) {
                        requiredAllocation = ((1 + indexMetaData.numberOfReplicas()) / 2);
                    }
                } else if ("one".equals(initialShards)) {
                    requiredAllocation = 1;
                } else if ("full".equals(initialShards) || "all".equals(initialShards)) {
                    requiredAllocation = indexMetaData.numberOfReplicas() + 1;
                } else if ("full-1".equals(initialShards) || "all-1".equals(initialShards)) {
                    if (indexMetaData.numberOfReplicas() > 1) {
                        requiredAllocation = indexMetaData.numberOfReplicas();
                    }
                } else {
                    requiredAllocation = Integer.parseInt(initialShards);
                }
            } catch (Exception e) {
                logger.warn("[{}][{}] failed to derived initial_shards from value {}, ignore allocation for {}", shard.index(), shard.id(), initialShards, shard);
            }
        }

        return nodesAndVersions.allocationsFound >= requiredAllocation;
    }

    /**
     * Based on the nodes and versions, build the list of yes/no/throttle nodes that the shard applies to.
     */
    private NodesToAllocate buildNodesToAllocate(ShardRouting shard, RoutingAllocation allocation, NodesAndVersions nodesAndVersions) {
        List<DiscoveryNode> yesNodes = new ArrayList<>();
        List<DiscoveryNode> throttledNodes = new ArrayList<>();
        List<DiscoveryNode> noNodes = new ArrayList<>();
        for (DiscoveryNode discoNode : nodesAndVersions.nodes) {
            RoutingNode node = allocation.routingNodes().node(discoNode.id());
            if (node == null) {
                continue;
            }

            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            if (decision.type() == Decision.Type.THROTTLE) {
                throttledNodes.add(discoNode);
            } else if (decision.type() == Decision.Type.NO) {
                noNodes.add(discoNode);
            } else {
                yesNodes.add(discoNode);
            }
        }
        return new NodesToAllocate(Collections.unmodifiableList(yesNodes), Collections.unmodifiableList(throttledNodes), Collections.unmodifiableList(noNodes));
    }

    /**
     * Builds a list of nodes and version
     */
    NodesAndVersions buildNodesAndVersions(ShardRouting shard, boolean recoveryOnAnyNode, Set<String> ignoreNodes,
                                           AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> shardState) {
        final Map<DiscoveryNode, Long> nodesWithVersion = new HashMap<>();
        int numberOfAllocationsFound = 0;
        long highestVersion = -1;
        for (TransportNodesListGatewayStartedShards.NodeGatewayStartedShards nodeShardState : shardState.getData().values()) {
            long version = nodeShardState.version();
            DiscoveryNode node = nodeShardState.getNode();

            if (ignoreNodes.contains(node.id())) {
                continue;
            }

            // -1 version means it does not exists, which is what the API returns, and what we expect to
            if (nodeShardState.storeException() == null) {
                logger.trace("[{}] on node [{}] has version [{}] of shard", shard, nodeShardState.getNode(), version);
            } else {
                // when there is an store exception, we disregard the reported version and assign it as -1 (same as shard does not exist)
                logger.trace("[{}] on node [{}] has version [{}] but the store can not be opened, treating as version -1", nodeShardState.storeException(), shard, nodeShardState.getNode(), version);
                version = -1;
            }

            if (recoveryOnAnyNode) {
                numberOfAllocationsFound++;
                if (version > highestVersion) {
                    highestVersion = version;
                }
                // We always put the node without clearing the map
                nodesWithVersion.put(node, version);
            } else if (version != -1) {
                numberOfAllocationsFound++;
                // If we've found a new "best" candidate, clear the
                // current candidates and add it
                if (version > highestVersion) {
                    highestVersion = version;
                    nodesWithVersion.clear();
                    nodesWithVersion.put(node, version);
                } else if (version == highestVersion) {
                    // If the candidate is the same, add it to the
                    // list, but keep the current candidate
                    nodesWithVersion.put(node, version);
                }
            }
        }
        // Now that we have a map of nodes to versions along with the
        // number of allocations found (and not ignored), we need to sort
        // it so the node with the highest version is at the beginning
        List<DiscoveryNode> nodesWithHighestVersion = new ArrayList<>();
        nodesWithHighestVersion.addAll(nodesWithVersion.keySet());
        CollectionUtil.timSort(nodesWithHighestVersion, new Comparator<DiscoveryNode>() {
            @Override
            public int compare(DiscoveryNode o1, DiscoveryNode o2) {
                return Long.compare(nodesWithVersion.get(o2), nodesWithVersion.get(o1));
            }
        });

        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("[");
            for (DiscoveryNode n : nodesWithVersion.keySet()) {
                sb.append("[").append(n.getName()).append("]").append(" -> ").append(nodesWithVersion.get(n)).append(", ");
            }
            sb.append("]");
            logger.trace("{} candidates for allocation: {}", shard, sb.toString());
        }

        return new NodesAndVersions(Collections.unmodifiableList(nodesWithHighestVersion), numberOfAllocationsFound, highestVersion);
    }

    /**
     * Return {@code true} if the index is configured to allow shards to be
     * recovered on any node
     */
    private boolean recoverOnAnyNode(@IndexSettings Settings idxSettings) {
        return IndexMetaData.isOnSharedFilesystem(idxSettings) &&
                idxSettings.getAsBoolean(IndexMetaData.SETTING_SHARED_FS_ALLOW_RECOVERY_ON_ANY_NODE, false);
    }

    protected abstract AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation);

    static class NodesAndVersions {
        public final List<DiscoveryNode> nodes;
        public final int allocationsFound;
        public final long highestVersion;

        public NodesAndVersions(List<DiscoveryNode> nodes, int allocationsFound, long highestVersion) {
            this.nodes = nodes;
            this.allocationsFound = allocationsFound;
            this.highestVersion = highestVersion;
        }
    }

    static class NodesToAllocate {
        final List<DiscoveryNode> yesNodes;
        final List<DiscoveryNode> throttleNodes;
        final List<DiscoveryNode> noNodes;

        public NodesToAllocate(List<DiscoveryNode> yesNodes, List<DiscoveryNode> throttleNodes, List<DiscoveryNode> noNodes) {
            this.yesNodes = yesNodes;
            this.throttleNodes = throttleNodes;
            this.noNodes = noNodes;
        }
    }
}
