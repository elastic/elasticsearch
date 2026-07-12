/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.EstimatedHeapUsage;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.core.Nullable;

public interface EstimatedHeapUsageAllocationDeciderNodeThingo {
    @Nullable
    Decision decisionForNode(RoutingNode node);

    @Nullable
    EstimatedHeapUsage heapUsageForNode(RoutingAllocation allocation, RoutingNode node);
}
