/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * <p>Cluster routing package provides routing information and manages shard
 * allocations in cluster. The routing part contains two different views on
 * shards in cluster {@link org.elasticsearch.cluster.routing.RoutingTable} and
 * {@link org.elasticsearch.cluster.routing.RoutingNodes}. RoutingTable provides
 * view from index to shard to node, and RoutingNodes provides view from node to
 * shard. Shard allocation is a process of assigning and moving shards between
 * nodes. For more details about allocation see {@link
 * org.elasticsearch.cluster.routing.allocation}.</p>
 *
 * <b>Routing Tables</b>
 *
 * <p>Routing tables provide a view on global cluster state from index
 * perspective. It's a mapping from indices to shards and shards to nodes, where
 * relationship between shard and node might not exists if shard allocation is
 * not possible. Routing table represents desired shards layout and current
 * state of shards.</p>
 *
 * <p>As class hierarchy goes - at the top level there is a {@link
 * org.elasticsearch.cluster.routing.RoutingTable}. RoutingTable is a part of
 * {@link org.elasticsearch.cluster.ClusterState}. It contains mapping between
 * indices and {@link org.elasticsearch.cluster.routing.IndexRoutingTable}.
 * {@link org.elasticsearch.cluster.routing.IndexRoutingTable} contains routing
 * information about all shards in that index - {@link
 * org.elasticsearch.cluster.routing.IndexShardRoutingTable}. Each shard can
 * have one or more instances - primary and replicas. IndexShardRoutingTable
 * contains information about all shard instances for specific shard id - {@link
 * org.elasticsearch.cluster.routing.ShardRouting}.</p>
 *
 * <b>Routing Nodes</b>
 *
 * <p>{@link org.elasticsearch.cluster.routing.RoutingNode} provides view to
 * shard routing from node perspective. Each RoutingNode is a {@link
 * org.elasticsearch.cluster.node.DiscoveryNode}, and contains information about
 * all shards and their state on this node. {@link
 * org.elasticsearch.cluster.routing.RoutingNodes} (plural) provide another view
 * on routing from nodes perspective - such as finding all shards by specific
 * node or finding all nodes for specific shard.</p>
 *
 * <b>Reroute</b>
 *
 * <p>Reroute is a process of routing change in the cluster. It will start
 * allocation process, which might assign or move shards around. When cluster
 * has an update that impacts routing (for example: node join cluster, index
 * created, snapshot restored, ...) the reroute process must be triggered. There
 * are 2 ways to trigger reroute - batched(recommended) with {@link
 * org.elasticsearch.cluster.routing.BatchedRerouteService} and non-batch {@link
 * org.elasticsearch.cluster.routing.allocation.AllocationService}.</p>
 */
package org.elasticsearch.cluster.routing;
