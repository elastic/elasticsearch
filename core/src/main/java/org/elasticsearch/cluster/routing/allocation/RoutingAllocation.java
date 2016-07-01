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

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;

/**
 * The {@link RoutingAllocation} keep the state of the current allocation
 * of shards and holds the {@link AllocationDeciders} which are responsible
 *  for the current routing state.
 */
public class RoutingAllocation {

    /**
     * this class is used to describe results of a {@link RoutingAllocation}
     */
    public static class Result {

        private final boolean changed;

        private final RoutingTable routingTable;

        private final MetaData metaData;

        private final RoutingExplanations explanations;

        /**
         * Creates a new {@link RoutingAllocation.Result}
         * @param changed a flag to determine whether the actual {@link RoutingTable} has been changed
         * @param routingTable the {@link RoutingTable} this Result references
         * @param metaData the {@link MetaData} this Result references
         */
        public Result(boolean changed, RoutingTable routingTable, MetaData metaData) {
            this(changed, routingTable, metaData, new RoutingExplanations());
        }

        /**
         * Creates a new {@link RoutingAllocation.Result}
         * @param changed a flag to determine whether the actual {@link RoutingTable} has been changed
         * @param routingTable the {@link RoutingTable} this Result references
         * @param metaData the {@link MetaData} this Result references
         * @param explanations Explanation for the reroute actions
         */
        public Result(boolean changed, RoutingTable routingTable, MetaData metaData, RoutingExplanations explanations) {
            this.changed = changed;
            this.routingTable = routingTable;
            this.metaData = metaData;
            this.explanations = explanations;
        }

        /** determine whether the actual {@link RoutingTable} has been changed
         * @return <code>true</code> if the {@link RoutingTable} has been changed by allocation. Otherwise <code>false</code>
         */
        public boolean changed() {
            return this.changed;
        }

        /**
         * Get the {@link MetaData} referenced by this result
         * @return referenced {@link MetaData}
         */
        public MetaData metaData() {
            return metaData;
        }

        /**
         * Get the {@link RoutingTable} referenced by this result
         * @return referenced {@link RoutingTable}
         */
        public RoutingTable routingTable() {
            return routingTable;
        }

        /**
         * Get the explanation of this result
         * @return explanation
         */
        public RoutingExplanations explanations() {
            return explanations;
        }
    }

    private final AllocationDeciders deciders;

    private final RoutingNodes routingNodes;

    private final MetaData metaData;

    private final RoutingTable routingTable;

    private final DiscoveryNodes nodes;

    private final ImmutableOpenMap<String, ClusterState.Custom> customs;

    private final AllocationExplanation explanation = new AllocationExplanation();

    private final ClusterInfo clusterInfo;

    private Map<ShardId, Set<String>> ignoredShardToNodes = null;

    private boolean ignoreDisable = false;

    private final boolean retryFailed;

    private boolean debugDecision = false;

    private boolean hasPendingAsyncFetch = false;

    private final long currentNanoTime;


    /**
     * Creates a new {@link RoutingAllocation}
     *  @param deciders {@link AllocationDeciders} to used to make decisions for routing allocations
     * @param routingNodes Routing nodes in the current cluster
     * @param clusterState cluster state before rerouting
     * @param currentNanoTime the nano time to use for all delay allocation calculation (typically {@link System#nanoTime()})
     */
    public RoutingAllocation(AllocationDeciders deciders, RoutingNodes routingNodes, ClusterState clusterState, ClusterInfo clusterInfo, long currentNanoTime, boolean retryFailed) {
        this.deciders = deciders;
        this.routingNodes = routingNodes;
        this.metaData = clusterState.metaData();
        this.routingTable = clusterState.routingTable();
        this.nodes = clusterState.nodes();
        this.customs = clusterState.customs();
        this.clusterInfo = clusterInfo;
        this.currentNanoTime = currentNanoTime;
        this.retryFailed = retryFailed;
    }

    /** returns the nano time captured at the beginning of the allocation. used to make sure all time based decisions are aligned */
    public long getCurrentNanoTime() {
        return currentNanoTime;
    }

    /**
     * Get {@link AllocationDeciders} used for allocation
     * @return {@link AllocationDeciders} used for allocation
     */
    public AllocationDeciders deciders() {
        return this.deciders;
    }

    /**
     * Get routing table of current nodes
     * @return current routing table
     */
    public RoutingTable routingTable() {
        return routingTable;
    }

    /**
     * Get current routing nodes
     * @return routing nodes
     */
    public RoutingNodes routingNodes() {
        return routingNodes;
    }

    /**
     * Get metadata of routing nodes
     * @return Metadata of routing nodes
     */
    public MetaData metaData() {
        return metaData;
    }

    /**
     * Get discovery nodes in current routing
     * @return discovery nodes
     */
    public DiscoveryNodes nodes() {
        return nodes;
    }

    public ClusterInfo clusterInfo() {
        return clusterInfo;
    }

    public <T extends ClusterState.Custom> T custom(String key) {
        return (T)customs.get(key);
    }

    /**
     * Get explanations of current routing
     * @return explanation of routing
     */
    public AllocationExplanation explanation() {
        return explanation;
    }

    public void ignoreDisable(boolean ignoreDisable) {
        this.ignoreDisable = ignoreDisable;
    }

    public boolean ignoreDisable() {
        return this.ignoreDisable;
    }

    public void debugDecision(boolean debug) {
        this.debugDecision = debug;
    }

    public boolean debugDecision() {
        return this.debugDecision;
    }

    public void addIgnoreShardForNode(ShardId shardId, String nodeId) {
        if (ignoredShardToNodes == null) {
            ignoredShardToNodes = new HashMap<>();
        }
        Set<String> nodes = ignoredShardToNodes.get(shardId);
        if (nodes == null) {
            nodes = new HashSet<>();
            ignoredShardToNodes.put(shardId, nodes);
        }
        nodes.add(nodeId);
    }

    public boolean shouldIgnoreShardForNode(ShardId shardId, String nodeId) {
        if (ignoredShardToNodes == null) {
            return false;
        }
        Set<String> nodes = ignoredShardToNodes.get(shardId);
        return nodes != null && nodes.contains(nodeId);
    }

    public Set<String> getIgnoreNodes(ShardId shardId) {
        if (ignoredShardToNodes == null) {
            return emptySet();
        }
        Set<String> ignore = ignoredShardToNodes.get(shardId);
        if (ignore == null) {
            return emptySet();
        }
        return unmodifiableSet(new HashSet<>(ignore));
    }

    /**
     * Create a routing decision, including the reason if the debug flag is
     * turned on
     * @param decision decision whether to allow/deny allocation
     * @param deciderLabel a human readable label for the AllocationDecider
     * @param reason a format string explanation of the decision
     * @param params format string parameters
     */
    public Decision decision(Decision decision, String deciderLabel, String reason, Object... params) {
        if (debugDecision()) {
            return Decision.single(decision.type(), deciderLabel, reason, params);
        } else {
            return decision;
        }
    }

    /**
     * Returns <code>true</code> iff the current allocation run has not processed all of the in-flight or available
     * shard or store fetches. Otherwise <code>true</code>
     */
    public boolean hasPendingAsyncFetch() {
        return hasPendingAsyncFetch;
    }

    /**
     * Sets a flag that signals that current allocation run has not processed all of the in-flight or available shard or store fetches.
     * This state is anti-viral and can be reset in on allocation run.
     */
    public void setHasPendingAsyncFetch() {
        this.hasPendingAsyncFetch = true;
    }

    public boolean isRetryFailed() {
        return retryFailed;
    }
}
