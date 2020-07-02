/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

/**
 * A service to decide for a specific decider.
 */
public interface AutoscalingDeciderService<D extends AutoscalingDeciderConfiguration> {

    /**
     * The name of the autoscaling decider.
     *
     * @return the name
     */
    String name();

    /**
     * Whether or not to scale based on the current state.
     *
     * @param context provides access to information about current state
     * @return the autoscaling decision
     */
    AutoscalingDecision scale(D decider, AutoscalingDeciderContext context);

}
