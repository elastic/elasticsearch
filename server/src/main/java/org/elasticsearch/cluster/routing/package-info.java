/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * <p>The cluster routing package provides routing information and manages shard
 * allocations in a cluster. The routing part contains two different views of
 * shards in {@link org.elasticsearch.cluster.routing.RoutingTable} and
 * {@link org.elasticsearch.cluster.routing.RoutingNodes}. RoutingTable provides
 * a view from index to shard to node. RoutingNodes provides view from node to
 * shard. Shard allocation is a process of assigning and moving shards between
 * nodes. For more details about allocation see
 * {@link org.elasticsearch.cluster.routing.allocation}.</p>
 *
 * <b>Routing Table</b>
 *
 * <p>The routing table provide a view of global cluster state from an index
 * perspective. It's a mapping from indices to shards and shards to nodes, where
 * the relationship between shard and node may not exist if a shard
 * allocation is not possible.
 *
 * <p>{@link org.elasticsearch.cluster.routing.RoutingTable} is the access
 * point to the routing information. RoutingTable is a part of the
 * {@link org.elasticsearch.cluster.ClusterState}. It maps index names to
 * {@link org.elasticsearch.cluster.routing.IndexRoutingTable}.
 * {@link org.elasticsearch.cluster.routing.IndexRoutingTable}, in turn,
 * holds a list of shards in that index,
 * {@link org.elasticsearch.cluster.routing.IndexShardRoutingTable}. Each
 * shard has one or more instances: a primary and replicas. An
 * IndexShardRoutingTable contains information about all shard instances for
 * a specific shard, {@link org.elasticsearch.cluster.routing.ShardRouting}.
 * The ShardRouting is the state of a shard instance in a cluster. There are
 * several states of ShardRouting: unassigned, initializing, relocating,
 * started.</p>
 *
 * An example of routing table:
 *
 * <pre>
 * {@code
 * └── ClusterState
 *     └── RoutingTable
 *         ├── index1-IndexRoutingTable
 *         │   ├── shard1-IndexShardRoutingTable
 *         │   │   ├── primary-ShardRouting
 *         │   │   │   └── STARTED-node1
 *         │   │   ├── replica1-ShardRouting
 *         │   │   │   └── INITIALIZING-node2
 *         │   │   └── replica2-ShardRouting
 *         │   │       └── UNASSIGNED
 *         │   └── shard2-IndexShardRoutingTable ...
 *         └── index2-IndexRoutingTable ...
 * }
 * </pre>
 *
 *
 * <b>Routing Nodes</b>
 *
 * <p>{@link org.elasticsearch.cluster.routing.RoutingNode} provides a view
 * from node to shard routing. There is a RoutingNode for every
 * {@link org.elasticsearch.cluster.node.DiscoveryNode}. It contains
 * information about  all shards and their state on this node.
 * {@link org.elasticsearch.cluster.routing.RoutingNodes} (plural) provide
 * aggregated view from all cluster nodes. It supports finding all shards by
 * specific node or finding all nodes for specific shard.</p>
 *
 * <b>Reroute</b>
 *
 * <p>Reroute is a process of routing update in the cluster. Routing update
 * may start allocation process, which assign or move shards around. When
 * cluster has an update that impacts routing (for example: node join
 * cluster, index created, snapshot restored, ...) the code that handles
 * cluster update should trigger reroute. There are 2 ways to trigger reroute -
 * batched with
 * {@link org.elasticsearch.cluster.routing.BatchedRerouteService} and
 * unbatched
 * {@link org.elasticsearch.cluster.routing.allocation.AllocationService}.
 * Batched reroute can combine multiple requests into one (used when starting
 * initializing shards). Unbatched reroute allows to mix other cluster state
 * changes that might be required to create or delete index.</p>
 */
package org.elasticsearch.cluster.routing;
