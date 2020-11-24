/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

/**
 * A service to decide for a specific decider.
 */
public interface AutoscalingDeciderService {

    /**
     * The name of the autoscaling decider.
     *
     * @return the name
     */
    String name();

    /**
     * Whether or not to scale based on the current state.
     *
     * @param configuration the configuration settings for a specific decider
     * @param context provides access to information about current state
     * @return result from this decider
     */
    AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context);

    List<Setting<?>> deciderSettings();
}
