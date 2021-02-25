/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.ccr.allocation;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.xpack.ccr.CcrSettings;

/**
 * An allocation decider that ensures primary shards of follower indices that are being bootstrapped are assigned to nodes that have the
 * remote cluster client role. This is necessary as those nodes reach out to the leader shards on the remote cluster to copy Lucene segment
 * files and periodically renew retention leases during the bootstrap.
 */
public final class CcrPrimaryFollowerAllocationDecider extends AllocationDecider {
    static final String NAME = "ccr_primary_follower";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        final IndexMetadata indexMetadata = allocation.metadata().index(shardRouting.index());
        if (CcrSettings.CCR_FOLLOWING_INDEX_SETTING.get(indexMetadata.getSettings()) == false) {
            return allocation.decision(Decision.YES, NAME, "shard is not a follower and is not under the purview of this decider");
        }
        if (shardRouting.primary() == false) {
            return allocation.decision(Decision.YES, NAME, "shard is a replica follower and is not under the purview of this decider");
        }
        final RecoverySource recoverySource = shardRouting.recoverySource();
        if (recoverySource == null || recoverySource.getType() != RecoverySource.Type.SNAPSHOT) {
            return allocation.decision(Decision.YES, NAME,
                "shard is a primary follower but was bootstrapped already; hence is not under the purview of this decider");
        }
        if (node.node().isRemoteClusterClient() == false) {
            return allocation.decision(Decision.NO, NAME, "shard is a primary follower and being bootstrapped, but node does not have the "
                + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName() + " role");
        }
        return allocation.decision(Decision.YES, NAME,
            "shard is a primary follower and node has the " + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName() + " role");
    }
}
