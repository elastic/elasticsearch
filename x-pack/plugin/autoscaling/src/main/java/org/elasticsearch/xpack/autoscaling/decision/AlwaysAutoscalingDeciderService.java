/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.action.admin.indices.rollover.MetadataRolloverService;
import org.elasticsearch.common.inject.Inject;

public class AlwaysAutoscalingDeciderService implements AutoscalingDeciderService<AlwaysAutoscalingDecider> {

    private MetadataRolloverService rolloverService;

    @Inject
    public AlwaysAutoscalingDeciderService(MetadataRolloverService rolloverService) {
        this.rolloverService = rolloverService;
        assert rolloverService != null;
    }

    @Override
    public String name() {
        return AlwaysAutoscalingDecider.NAME;
    }

    @Override
    public AutoscalingDecision scale(AlwaysAutoscalingDecider decider, AutoscalingDeciderContext context) {
        return null;
    }
}
