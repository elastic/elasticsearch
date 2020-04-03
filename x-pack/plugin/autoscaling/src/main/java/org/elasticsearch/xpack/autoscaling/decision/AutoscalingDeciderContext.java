/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.action.admin.indices.rollover.MetadataRolloverService;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

public interface AutoscalingDeciderContext {
    // input
    public ClusterState state();
    public ClusterInfo info();

    // demonstrate service
    MetadataRolloverService rolloverService();

    // demonstrate specific calls
    void allocate(RoutingAllocation allocation);
    Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation);
    Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation);
}
