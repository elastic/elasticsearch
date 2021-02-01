/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

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
