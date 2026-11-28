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
 * Pins the primary shard of an index with {@code index.storage_format: parquet} to its node, and refuses replicas.
 *
 * <p>Peer recovery copies only the Lucene commit of a shard, which has no segments, so the local Parquet files ({@code pqlocal_*}) do
 * not move with a shard. They hold rows that are acknowledged and not uploaded yet, so a relocated primary would start with an empty
 * store and lose them without an error. A replica would be an empty copy of a shard that never receives an operation. A real handoff
 * (flush and block writes, or ship the local files) is future work, and until then the shard must stay where it is.
 *
 * <p>Every hook that can decide where a shard goes says no, for a parquet index only:
 * <ul>
 *   <li>{@code canAllocate} covers the balancer, the reconciler of the desired balance, the {@code move} and {@code allocate_replica}
 *       reroute commands, and the replica that a settings update creates. An <em>unassigned</em> primary stays allocatable: a new index
 *       needs a node, and after a restart the gateway allocator picks the node that holds the copy of the shard;</li>
 *   <li>{@code canForceAllocateDuringReplace} defaults to yes and overrides every other {@code NO} when a node is being replaced, so
 *       this decider opts out as its documentation asks;</li>
 *   <li>{@code canRebalance} and {@code shouldAutoExpandToNode} keep the balancer from trying, and auto-expansion from asking.</li>
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
        return indexMetadata != null
            && ParquetFormatSettings.INDEX_FORMAT.get(indexMetadata.getSettings()) == ParquetFormatSettings.IndexFormat.PARQUET;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (isParquet(allocation, shardRouting) == false) {
            return YES_OTHER_FORMAT;
        }
        if (shardRouting.primary() == false) {
            return noReplicas(allocation, shardRouting);
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
        return shardRouting.primary() ? pinned(allocation, shardRouting) : noReplicas(allocation, shardRouting);
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
        return allocation.decision(Decision.NO, NAME, "an index with the parquet format does not support replicas");
    }

    private static Decision noReplicas(RoutingAllocation allocation, ShardRouting shardRouting) {
        return allocation.decision(
            Decision.NO,
            NAME,
            "[%s] uses the parquet format, which does not support replicas: the engine has no operations that a replica could recover",
            shardRouting.index().getName()
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
