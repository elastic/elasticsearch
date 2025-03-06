package org.elasticsearch.cluster.routing.allocation.decider;

import java.util.Optional;
import java.util.Set;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.NodeGroup;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;

public class ShrinkGroupAllocationDecider extends AllocationDecider {
    public static final String NAME = "shrink_group";

    public ShrinkGroupAllocationDecider() {}

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        IndexMetadata indexMetadata = allocation.metadata().indexMetadata(shardRouting.index());
        if (shardRouting.unassigned() && shardRouting.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            NodeGroup initialRecoveryGroup = indexMetadata.getInitialRecoveryNodeGroup();
            if (initialRecoveryGroup != null && !initialRecoveryGroup.matchShard(node.node().getId(), shardRouting.getId())) {
                return allocation.decision(Decision.NO, NAME, "initial allocation of this shard is only allowed on ["+initialRecoveryGroup.nodeForShard(shardRouting.getId())+"]");
            }
        }
        return decideGroupShard(shardRouting, node.node(), allocation);
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return decideGroupIndex(indexMetadata, node.node(), allocation);
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return decideGroupShard(shardRouting, node.node(), allocation);
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        return decideGroupIndex(indexMetadata, node, allocation);
    }

    private Decision decideGroupIndex(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        NodeGroup group = indexMetadata.getShrinkGroup();
        if (group == null) {
            return allocation.decision(Decision.YES, NAME, "no shrink group configured");
        }

        if (!group.toList().contains(node.getId())) {
            return allocation.decision(Decision.NO, NAME, "shrink group does not allow allocation to this node");
        }

        return allocation.decision(Decision.YES, NAME, "index allows allocation to this node");
    }

    private Decision decideGroupShard(ShardRouting shardRouting, DiscoveryNode node, RoutingAllocation allocation) {
        IndexMetadata indexMetadata = allocation.metadata().indexMetadata(shardRouting.index());
        NodeGroup group = indexMetadata.getShrinkGroup();
        if (group == null) {
            return allocation.decision(Decision.YES, NAME, "no shrink group configured");
        }

        if (!shardRouting.primary()) {
            return allocation.decision(Decision.YES, NAME, "shrink group does not apply to replica nodes");
        }

        if (!group.matchShard(node.getId(), shardRouting.getId())) {
            return allocation.decision(Decision.NO, NAME, "allocation of this shard is only allowed on ["+group.nodeForShard(shardRouting.getId())+"]");
        }

        return allocation.decision(Decision.YES, NAME, "index allows allocation to this node");
    }

    @Override
    public Optional<Set<String>> getForcedInitialShardAllocationToNodes(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (shardRouting.unassigned() && shardRouting.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            IndexMetadata indexMetadata = allocation.metadata().indexMetadata(shardRouting.index());
            NodeGroup initialRecoveryGroup = indexMetadata.getInitialRecoveryNodeGroup();
            if (initialRecoveryGroup != null) {
                return Optional.of(Set.of(initialRecoveryGroup.nodeForShard(shardRouting.getId())));
            }
        }
        return super.getForcedInitialShardAllocationToNodes(shardRouting, allocation);
    }
}
