/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.xpack.autoscaling.AutoscalingExtension;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Collection;

public class MlAutoscalingExtension implements AutoscalingExtension {
    private final MachineLearning plugin;

    public MlAutoscalingExtension() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public MlAutoscalingExtension(MachineLearning plugin) {
        this.plugin = plugin;
    }

    @Override
    public Collection<AutoscalingDeciderService> deciders() {
        return plugin.deciders();
    }
}
