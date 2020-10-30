/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;

public interface PolicyValidator {
    void validate(AutoscalingPolicy policy);
}
