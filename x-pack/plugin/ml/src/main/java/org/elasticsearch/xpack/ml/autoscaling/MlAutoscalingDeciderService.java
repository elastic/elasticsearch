/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecision;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisionType;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

public class MlAutoscalingDeciderService implements AutoscalingDeciderService<MlAutoscalingDecider> {
    private final MlMemoryTracker memoryTracker;

    @Inject
    public MlAutoscalingDeciderService(MlMemoryTracker memoryTracker) {
        assert memoryTracker != null;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public String name() {
        return MlAutoscalingDecider.NAME;
    }

    @Override
    public AutoscalingDecision scale(MlAutoscalingDecider decider, AutoscalingDeciderContext context) {
        return new AutoscalingDecision(name(), AutoscalingDecisionType.NO_SCALE, "not implemented");
    }
}
