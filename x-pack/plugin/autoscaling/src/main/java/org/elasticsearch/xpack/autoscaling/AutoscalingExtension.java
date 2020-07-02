/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderConfiguration;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderService;

import java.util.Collection;

public interface AutoscalingExtension {
    /**
     * Get the list of decider services for this plugin. This is called after createComponents has been called.
     * @return list of decider services
     */
    Collection<AutoscalingDeciderService<? extends AutoscalingDeciderConfiguration>> deciders();
}
