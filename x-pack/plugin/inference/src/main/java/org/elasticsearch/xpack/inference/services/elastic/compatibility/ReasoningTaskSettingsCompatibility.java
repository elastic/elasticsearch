/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.compatibility;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.InferenceFeatures;

import java.util.Map;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;

public class ReasoningTaskSettingsCompatibility implements Compatibility {

    @Override
    public InferenceService.ClusterCompatibility clusterCompatibility(FeatureService featureService, ClusterState state, TaskType taskType, Map<String, Object> config) {
        if (taskType == TaskType.CHAT_COMPLETION
            && config.get(ModelConfigurations.TASK_SETTINGS) instanceof Map<?, ?> ts
            && ts.containsKey(REASONING_FIELD)
            && featureService.clusterHasFeature(state, InferenceFeatures.INFERENCE_ELASTIC_REASONING_TASK_SETTINGS) == false) {
            return InferenceService.ClusterCompatibility.unsupported(
                "The reasoning field in task_settings is not supported by all nodes in the cluster; "
                    + "please finish upgrading before creating an endpoint with reasoning task_settings"
            );
        }

        return InferenceService.ClusterCompatibility.supported();
    }
}
