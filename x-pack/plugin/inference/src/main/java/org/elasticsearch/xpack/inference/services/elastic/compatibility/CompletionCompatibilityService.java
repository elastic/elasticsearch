/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.compatibility;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceFeatureService;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.ImmutableEmptyTaskSettings;

import java.util.Map;
import java.util.Objects;

/**
 * Service that provides compatibility strategies for task settings based on the task type and feature availability.
 */
public class CompletionCompatibilityService {

    public static final String REASONING_FIELD_UNSUPPORTED_MESSAGE =
        "The reasoning field in task_settings is not supported by all nodes in the cluster; "
            + "please finish upgrading before using the reasoning field";

    private final InferenceFeatureService featureService;

    public CompletionCompatibilityService(InferenceFeatureService featureService) {
        this.featureService = Objects.requireNonNull(featureService);
    }

    /**
     * Checks whether the provided cluster state supports the model's task settings.
     */
    public InferenceService.ClusterCompatibility clusterCompatibility(ClusterState state, ElasticInferenceServiceModel model) {
        if (model.getTaskSettings() instanceof ElasticInferenceServiceChatCompletionTaskSettings ts
            && ts.isEmpty() == false
            && featureService.hasFeature(state, InferenceFeatures.INFERENCE_ELASTIC_REASONING_TASK_SETTINGS) == false) {
            return InferenceService.ClusterCompatibility.unsupported(REASONING_FIELD_UNSUPPORTED_MESSAGE);
        }

        return InferenceService.ClusterCompatibility.supported();
    }

    /**
     * Strategy interface for creating {@link TaskSettings} based on the task type and feature availability.
     */
    public abstract static class TaskSettingsStrategy {
        protected final TaskType taskType;

        public TaskSettingsStrategy(TaskType taskType) {
            this.taskType = taskType;
        }

        /**
         * Creates the appropriate {@link TaskSettings} based on the provided task settings map and context.
         */
        public abstract TaskSettings createTaskSettings(Map<String, Object> taskSettings, ConfigurationParseContext context);
    }

    /**
     * Returns the appropriate {@link TaskSettingsStrategy} based on the task type and feature availability.
     */
    public TaskSettingsStrategy getTaskSettingsStrategy(TaskType taskType) {
        // If the reasoning task settings is not supported by the whole cluster we'll need to continue to return EmptyTaskSettings for BWC
        // but we'll want to make sure the reasoning fields are not present. We can't return ImmutableEmptyTaskSettings because
        // if the PUT result (ModelConfiguration serialization) is sent to a node that hasn't been upgraded, it won't recognize the
        // ImmutableEmptyTaskSettings named writeable yet.
        // Once the cluster is updated, we can begin returning ImmutableEmptyTaskSettings for Completion
        if (featureService.hasFeature(InferenceFeatures.INFERENCE_ELASTIC_REASONING_TASK_SETTINGS) == false) {
            return new EnforceEmptyTaskSettingsStrategy(taskType);
        }

        return switch (taskType) {
            case CHAT_COMPLETION -> new ReasoningTaskSettingsStrategy(taskType);
            default -> new ImmutableEmptyTaskSettingsStrategy(taskType);
        };
    }

    /**
     * A strategy that enforces that task settings are empty. If it is non-empty, it throws an exception.
     */
    static class EnforceEmptyTaskSettingsStrategy extends TaskSettingsStrategy {
        public EnforceEmptyTaskSettingsStrategy(TaskType taskType) {
            super(taskType);
        }

        @Override
        public TaskSettings createTaskSettings(Map<String, Object> taskSettings, ConfigurationParseContext context) {
            if (taskSettings.isEmpty() || context == ConfigurationParseContext.PERSISTENT) {
                return EmptyTaskSettings.INSTANCE;
            }

            throw new ElasticsearchStatusException(
                "[{}] Configuration contains unknown settings {}",
                RestStatus.BAD_REQUEST,
                ModelConfigurations.TASK_SETTINGS,
                taskSettings.keySet()
            );
        }
    }

    /**
     * A strategy that creates {@link ElasticInferenceServiceChatCompletionTaskSettings} for chat completion tasks.
     */
    static class ReasoningTaskSettingsStrategy extends TaskSettingsStrategy {
        public ReasoningTaskSettingsStrategy(TaskType taskType) {
            super(taskType);
        }

        @Override
        public TaskSettings createTaskSettings(Map<String, Object> taskSettings, ConfigurationParseContext context) {
            return ElasticInferenceServiceChatCompletionTaskSettings.fromMap(taskSettings, taskType, context);
        }
    }

    /**
     * A strategy that creates {@link ImmutableEmptyTaskSettings} to enforce that creation and update do not provide any settings.
     */
    static class ImmutableEmptyTaskSettingsStrategy extends TaskSettingsStrategy {
        public ImmutableEmptyTaskSettingsStrategy(TaskType taskType) {
            super(taskType);
        }

        @Override
        public TaskSettings createTaskSettings(Map<String, Object> taskSettings, ConfigurationParseContext context) {
            return ImmutableEmptyTaskSettings.fromMap(taskSettings, context);
        }
    }
}
