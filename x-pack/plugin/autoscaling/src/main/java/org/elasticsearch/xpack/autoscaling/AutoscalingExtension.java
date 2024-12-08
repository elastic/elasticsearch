/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;

import java.util.Collection;

public interface AutoscalingExtension {
    /**
     * Get the list of decider services for this plugin. This is called after createComponents has been called.
     * @return list of decider services
     */
    Collection<AutoscalingDeciderService> deciders();
}
