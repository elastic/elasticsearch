/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecider;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderService;

import java.util.Set;

public class AutoscalingDeciderServices {
    private static final Logger logger = LogManager.getLogger(AutoscalingDeciderServices.class);
    private Set<AutoscalingDeciderService<? extends AutoscalingDecider>> deciders;

    @Inject
    public AutoscalingDeciderServices(Set<AutoscalingDeciderService<? extends AutoscalingDecider>> deciders) {
        this.deciders = deciders;
        assert this.deciders.size() >= 1;
        logger.info("Deciders: " + deciders);
    }

}
