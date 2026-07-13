/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;

/**
 * Validator to check autoscaling policies
 */
public interface PolicyValidator {
    /**
     * Validate the given policy, e.g., check decider names, setting names and values.
     * @param policy the policy to validate
     */
    void validate(AutoscalingPolicy policy);
}
