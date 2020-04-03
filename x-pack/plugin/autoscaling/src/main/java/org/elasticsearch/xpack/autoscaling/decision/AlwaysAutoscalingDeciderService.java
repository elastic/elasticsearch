/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.action.admin.indices.rollover.MetadataRolloverService;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.common.inject.Inject;

public class AlwaysAutoscalingDeciderService implements AutoscalingDeciderService<AlwaysAutoscalingDecider> {
    private MetadataRolloverService rolloverService;
    private ShardsAllocator shardsAllocator;

    @Inject
    public AlwaysAutoscalingDeciderService(MetadataRolloverService rolloverService, ShardsAllocator shardsAllocator) {
        this.rolloverService = rolloverService;
        this.shardsAllocator = shardsAllocator;
    }

    @Override
    public AutoscalingDecision scale(AlwaysAutoscalingDecider decider, AutoscalingDeciderContext context) {
        // should have access to decider args...
        String name = decider.name();
        return new AutoscalingDecision(decider.name(), AutoscalingDecisionType.SCALE_UP, "always");
    }
}
