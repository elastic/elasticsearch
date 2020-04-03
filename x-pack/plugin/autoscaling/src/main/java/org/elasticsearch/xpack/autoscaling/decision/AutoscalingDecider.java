/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;

/**
 * Represents an autoscaling decider, a component that determines whether or not to scale.
 */
public interface AutoscalingDecider extends ToXContentObject, NamedWriteable {

    /**
     * The name of the autoscaling decider.
     *
     * @return the name
     */
    String name();

    /**
     * Bind this decider to services. The resulting bound decider will be reused for multiple scaling decisions as long as the policy does
     * not change.
     *
     * @param registry provides access to services.
     * @return a bound decider that can make scaling decisions.
     */
    BoundDecider bind(AutoscalingDeciderServiceRegistry registry);

    interface AutoscalingDeciderServiceRegistry {
        <T> T get(Class<T> service);
    }
    // todo: maybe find a better name?
    interface BoundDecider {
        /**
         * Whether or not to scale based on the current state.
         *
         * @param context provides access to information about current state
         * @return the autoscaling decision
         */
        AutoscalingDecision scale(AutoscalingDeciderContext context);
    }

}
