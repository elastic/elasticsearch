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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceChatCompletionTaskSettings;

public class ReasoningTaskSettingsCompatibility implements Compatibility {

    public static final String REASONING_FIELD_UNSUPPORTED_MESSAGE =
        "The reasoning field in task_settings is not supported by all nodes in the cluster; "
            + "please finish upgrading before using the reasoning field";

    @Override
    public InferenceService.ClusterCompatibility clusterCompatibility(
        FeatureService featureService,
        ClusterState state,
        TaskType taskType,
        ElasticInferenceServiceModel elasticInferenceServiceModel
    ) {
        if (taskType == TaskType.CHAT_COMPLETION
            && elasticInferenceServiceModel.getTaskSettings() instanceof ElasticInferenceServiceChatCompletionTaskSettings ts
            && ts.isEmpty() == false
            && featureService.clusterHasFeature(state, InferenceFeatures.INFERENCE_ELASTIC_REASONING_TASK_SETTINGS) == false) {
            return InferenceService.ClusterCompatibility.unsupported(REASONING_FIELD_UNSUPPORTED_MESSAGE);
        }

        return InferenceService.ClusterCompatibility.supported();
    }
}
