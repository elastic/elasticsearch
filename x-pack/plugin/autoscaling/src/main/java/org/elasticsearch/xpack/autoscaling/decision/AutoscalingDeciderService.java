/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

/**
 * The service for a decider that can actually make a decision. There is a 1-1 relationship between decider and decider-service, similar
 * to request and transport-service.
 *
 * The drawback here is that a separate registration of the services is necessary. In principle, that could register the necessary deciders
 * too.
 */
public interface AutoscalingDeciderService<T extends AutoscalingDecider> {

    /**
     * Whether or not to scale based on the current state.
     *
     * @param context provides access to information about current state
     * @return the autoscaling decision
     */
    AutoscalingDecision scale(T decider, AutoscalingDeciderContext context);

}
