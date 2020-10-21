/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.xpack.autoscaling.AutoscalingExtension;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderConfiguration;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Collection;

public class MlAutoscalingExtension implements AutoscalingExtension {
    private final MachineLearning plugin;

    public MlAutoscalingExtension(MachineLearning plugin) {
        this.plugin = plugin;
    }

    @Override
    public Collection<AutoscalingDeciderService<? extends AutoscalingDeciderConfiguration>> deciders() {
        return plugin.deciders();
    }
}
