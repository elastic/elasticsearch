/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.cluster.ClusterState;

public interface AutoscalingDeciderContext {
    ClusterState state();

    /**
     * Return current capacity of tier. Can be null if the capacity of some nodes is unavailable. If a decider relies on this value and
     * gets a null current capacity, it should return a decision with a null requiredCapacity (undecided).
     */
    AutoscalingCapacity currentCapacity();
}
