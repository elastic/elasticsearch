/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.inject.Inject;

public class AlwaysAutoscalingDeciderService implements AutoscalingDeciderService<AlwaysAutoscalingDecider> {

    @Inject
    public AlwaysAutoscalingDeciderService() {}

    @Override
    public String name() {
        return AlwaysAutoscalingDecider.NAME;
    }

    @Override
    public AutoscalingDecision scale(AlwaysAutoscalingDecider decider, AutoscalingDeciderContext context) {
        return new AutoscalingDecision(name(), AutoscalingDecisionType.SCALE_UP, "always");
    }
}
