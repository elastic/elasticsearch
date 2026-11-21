/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

/**
 * Pins the primary shard of an index with {@code index.storage_format: parquet} to its node, lets its search replicas go anywhere, and
 * refuses any other replica.
 *
 * <p>Peer recovery copies only the Lucene commit of a shard, which has no segments, so the local Parquet files ({@code pqlocal_*}) do
 * not move with a shard. They hold rows that are acknowledged and not uploaded yet, so a relocated primary would start with an empty
 * store and lose them without an error. A real handoff (flush and block writes, or ship the local files) is future work, and until then
 * the primary must stay where it is.
 *
 * <p>The replicas of such an index are {@link ShardRouting.Role#SEARCH_ONLY} (see {@link ParquetShardRoutingRoleStrategy}): they never
 * receive an operation and read the uploaded files of the Iceberg table, so they have no local state and may be allocated, moved and
 * rebalanced like any shard. A replica with another role (which the role strategy never creates) would be an empty copy of a shard that
 * never receives an operation, and is refused.
 *
 * <p>For the primary, every hook that can decide where a shard goes says no:
 * <ul>
 *   <li>{@code canAllocate} covers the balancer, the reconciler of the desired balance, the {@code move} reroute command, and the
 *       allocators. An <em>unassigned</em> primary stays allocatable: a new index needs a node, and after a restart the gateway allocator
 *       picks the node that holds the copy of the shard;</li>
 *   <li>{@code canForceAllocateDuringReplace} defaults to yes and overrides every other {@code NO} when a node is being replaced, so
 *       this decider opts out as its documentation asks;</li>
 *   <li>{@code canRebalance} keeps the balancer from trying.</li>
 * </ul>
 *
 * <p>A primary whose node has to go (a shutdown of type remove or replace, an allocation filter, a disk watermark) therefore cannot
 * leave. The allocator reports that it cannot remain and finds no target, and the shard stays.
 */
public final class ParquetAllocationDecider extends AllocationDecider {

    public static final String NAME = "parquet_write_engine";

    private static final Decision YES_OTHER_FORMAT = Decision.single(Decision.Type.YES, NAME, "the index does not use the parquet format");

    private static boolean isParquet(RoutingAllocation allocation, ShardRouting shardRouting) {
        final IndexMetadata indexMetadata = allocation.metadata().indexMetadata(shardRouting.index());
        return isParquet(indexMetadata);
    }

    private static boolean isParquet(IndexMetadata indexMetadata) {
        return ParquetFormatSettings.isParquet(indexMetadata);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (isParquet(allocation, shardRouting) == false) {
            return YES_OTHER_FORMAT;
        }
        if (shardRouting.primary() == false) {
            return replica(allocation, shardRouting);
        }
        if (shardRouting.currentNodeId() == null || shardRouting.currentNodeId().equals(node.nodeId())) {
            return allocation.decision(Decision.YES, NAME, "the primary of a parquet index is not assigned yet, or stays on its node");
        }
        return pinned(allocation, shardRouting);
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (isParquet(allocation, shardRouting) == false) {
            return YES_OTHER_FORMAT;
        }
        return shardRouting.primary() ? pinned(allocation, shardRouting) : replica(allocation, shardRouting);
    }

    @Override
    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        // the caller checked that the shard's node is being replaced, and the answer of the default is yes: opt out
        return canAllocate(shardRouting, node, allocation);
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        if (isParquet(indexMetadata) == false) {
            return YES_OTHER_FORMAT;
        }
        // auto-expansion adds replicas, which the role strategy makes search replicas
        return allocation.decision(Decision.YES, NAME, "the replicas of an index with the parquet format are search replicas");
    }

    private static Decision replica(RoutingAllocation allocation, ShardRouting shardRouting) {
        if (shardRouting.role() == ShardRouting.Role.SEARCH_ONLY) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "a search replica of a parquet index reads the uploaded files of its table, and has no local state that pins it"
            );
        }
        return allocation.decision(
            Decision.NO,
            NAME,
            "[%s] uses the parquet format, whose replicas must be search replicas: a replica with the role [%s] would never receive an "
                + "operation",
            shardRouting.index().getName(),
            shardRouting.role()
        );
    }

    private static Decision pinned(RoutingAllocation allocation, ShardRouting shardRouting) {
        return allocation.decision(
            Decision.NO,
            NAME,
            "the primary of [%s] uses the parquet format and cannot move: its local files, with the rows that are not uploaded yet, do not "
                + "move with the shard",
            shardRouting.index().getName()
        );
    }
}
