/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.compatibility;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.EnforcingEmptyTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.ImmutableEmptyTaskSettings;

import java.util.Map;
import java.util.Objects;

/**
 * Service that provides compatibility strategies for task settings based on the task type and feature availability.
 */
public class CompletionCompatibilityService {

    public static final String REASONING_FIELD_UNSUPPORTED_MESSAGE = """
        The reasoning field in task_settings is not supported by all nodes in the cluster; \
        please finish upgrading before using the reasoning field""";

    private final ClusterService clusterService;
    private final FeatureService featureService;

    public CompletionCompatibilityService(ClusterService clusterService, FeatureService featureService) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.featureService = Objects.requireNonNull(featureService);
    }

    /**
     * Strategy interface for creating {@link TaskSettings} based on the task type and feature availability.
     */
    public abstract static class TaskSettingsStrategy {
        protected final TaskType taskType;

        TaskSettingsStrategy(TaskType taskType) {
            this.taskType = taskType;
        }

        /**
         * Creates the appropriate {@link TaskSettings} based on the provided task settings map and context.
         */
        public abstract TaskSettings createTaskSettings(Map<String, Object> taskSettings, ConfigurationParseContext context);
    }

    /**
     * Returns the empty {@link TaskSettings} instance to use for system-generated (preconfigured) completion/chat_completion
     * endpoints. Until the reasoning feature is available cluster-wide we must emit settings that serialize like
     * {@link EmptyTaskSettings} so that a not-yet-upgraded master can deserialize the persisted model; once the cluster is
     * fully upgraded we can emit {@link ImmutableEmptyTaskSettings}.
     */
    public TaskSettings emptyCompletionTaskSettings() {
        return featureService.clusterHasFeature(clusterService.state(), InferenceFeatures.INFERENCE_ELASTIC_REASONING_TASK_SETTINGS)
            ? ImmutableEmptyTaskSettings.INSTANCE
            : EnforcingEmptyTaskSettings.INSTANCE;
    }

    /**
     * Returns the appropriate {@link TaskSettingsStrategy} based on the task type and feature availability.
     */
    public TaskSettingsStrategy getTaskSettingsStrategy(TaskType taskType) {
        // If the reasoning task settings is not supported by the whole cluster we'll need to continue to return settings that
        // serialize like EmptyTaskSettings for BWC, but we'll want to make sure the reasoning fields are not present. We can't
        // return ImmutableEmptyTaskSettings because if the PUT result (ModelConfiguration serialization) is sent to a node that
        // hasn't been upgraded, it won't recognize the ImmutableEmptyTaskSettings named writeable yet. EnforcingEmptyTaskSettings
        // solves this: it serializes exactly like EmptyTaskSettings (old nodes read it back as such) but still rejects unknown
        // settings when updated, unlike EmptyTaskSettings itself.
        // Once the cluster is updated, we can begin returning ImmutableEmptyTaskSettings for Completion
        if (featureService.clusterHasFeature(
            clusterService.state(),
            InferenceFeatures.INFERENCE_ELASTIC_REASONING_TASK_SETTINGS
        ) == false) {
            return new EnforceEmptyTaskSettingsStrategy(taskType);
        }

        return switch (taskType) {
            case CHAT_COMPLETION -> new ReasoningTaskSettingsStrategy(taskType);
            case COMPLETION -> new ImmutableEmptyTaskSettingsStrategy(taskType);
            default -> throw new IllegalArgumentException("Unsupported task type: " + taskType);
        };
    }

    /**
     * A strategy that enforces that task settings are empty. If it is non-empty, it throws an exception.
     */
    static class EnforceEmptyTaskSettingsStrategy extends TaskSettingsStrategy {
        EnforceEmptyTaskSettingsStrategy(TaskType taskType) {
            super(taskType);
        }

        @Override
        public TaskSettings createTaskSettings(Map<String, Object> taskSettings, ConfigurationParseContext context) {
            return EnforcingEmptyTaskSettings.fromMap(taskSettings, context);
        }
    }

    /**
     * A strategy that creates {@link ElasticInferenceServiceChatCompletionTaskSettings} for chat completion tasks.
     */
    static class ReasoningTaskSettingsStrategy extends TaskSettingsStrategy {
        ReasoningTaskSettingsStrategy(TaskType taskType) {
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
        ImmutableEmptyTaskSettingsStrategy(TaskType taskType) {
            super(taskType);
        }

        @Override
        public TaskSettings createTaskSettings(Map<String, Object> taskSettings, ConfigurationParseContext context) {
            return ImmutableEmptyTaskSettings.fromMap(taskSettings, context);
        }
    }
}
