/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.allocation;

import co.elastic.elasticsearch.stateless.Stateless;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

import java.util.Set;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INDEX_ROLE;

public class StatelessAllocationDecider extends AllocationDecider {

    private static final String NAME = "stateless_shard_role";

    private static final Decision YES_SHARD_ROLE_MATCHES_NODE_ROLE = Decision.single(
        Decision.Type.YES,
        NAME,
        "shard role matches stateless node role"
    );

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return decideCanAllocateShardToNode(shardRouting, node, allocation);
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return decideCanAllocateShardToNode(shardRouting, node, allocation);
    }

    private Decision decideCanAllocateShardToNode(ShardRouting shardRouting, RoutingNode routingNode, RoutingAllocation allocation) {
        var roles = routingNode.node().getRoles();
        return canAllocateShardToNode(shardRouting, roles)
            ? YES_SHARD_ROLE_MATCHES_NODE_ROLE
            : allocation.decision(
                Decision.NO,
                NAME,
                "shard role [%s] does not match stateless node role [%s]",
                shardRouting.role().toString(),
                statelessNodeRole(roles)
            );
    }

    private boolean canAllocateShardToNode(ShardRouting shardRouting, Set<DiscoveryNodeRole> nodeRoles) {
        return (shardRouting.isPromotableToPrimary() && nodeRoles.contains(INDEX_ROLE))
            || (shardRouting.isSearchable() && nodeRoles.contains(DiscoveryNodeRole.SEARCH_ROLE));
    }

    private String statelessNodeRole(Set<DiscoveryNodeRole> roles) {
        return roles.stream().filter(Stateless.STATELESS_ROLES::contains).map(DiscoveryNodeRole::roleName).collect(joining(","));
    }
}
