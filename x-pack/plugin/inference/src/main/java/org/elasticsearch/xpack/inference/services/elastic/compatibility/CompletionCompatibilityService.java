/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.compatibility;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.EnforcingEmptyTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.ImmutableEmptyTaskSettings;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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

        /**
         * Creates the appropriate {@link TaskSettings} based on the provided reasoning configuration and the state of the cluster.
         */
        public abstract Optional<TaskSettings> createTaskSettings(@Nullable Reasoning reasoning);
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
            return new MixedClusterTaskSettingsStrategy(taskType);
        }

        return switch (taskType) {
            case CHAT_COMPLETION -> new ChatCompletionTaskSettingsStrategy(taskType);
            case COMPLETION -> new CompletionTaskSettingsStrategy(taskType);
            default -> throw new IllegalArgumentException("Unsupported task type: " + taskType);
        };
    }

    /**
     * A strategy to handle a mixed cluster.
     */
    static class MixedClusterTaskSettingsStrategy extends TaskSettingsStrategy {
        MixedClusterTaskSettingsStrategy(TaskType taskType) {
            super(taskType);
        }

        @Override
        public TaskSettings createTaskSettings(Map<String, Object> taskSettings, ConfigurationParseContext context) {
            return EnforcingEmptyTaskSettings.fromMap(taskSettings, context);
        }

        /**
         * A mixed cluster cannot support reasoning settings, so return empty if the reasoning field was provided or the
         * {@link EnforcingEmptyTaskSettings} to ensure backwards compatibility and that unknown fields are rejected.
         */
        @Override
        public Optional<TaskSettings> createTaskSettings(@Nullable Reasoning reasoning) {
            if (reasoning == null) {
                return Optional.of(EnforcingEmptyTaskSettings.INSTANCE);
            }

            return Optional.empty();
        }
    }

    /**
     * A strategy that creates {@link ElasticInferenceServiceChatCompletionTaskSettings} for chat completion tasks.
     */
    static class ChatCompletionTaskSettingsStrategy extends TaskSettingsStrategy {
        ChatCompletionTaskSettingsStrategy(TaskType taskType) {
            super(taskType);
        }

        @Override
        public TaskSettings createTaskSettings(Map<String, Object> taskSettings, ConfigurationParseContext context) {
            return ElasticInferenceServiceChatCompletionTaskSettings.fromMap(taskSettings, taskType, context);
        }

        /**
         * Reasoning settings are supported so create the task settings.
         */
        @Override
        public Optional<TaskSettings> createTaskSettings(@Nullable Reasoning reasoning) {
            return Optional.of(new ElasticInferenceServiceChatCompletionTaskSettings(reasoning));
        }
    }

    /**
     * A strategy that creates {@link ImmutableEmptyTaskSettings} to enforce that creation and update do not provide any settings.
     */
    static class CompletionTaskSettingsStrategy extends TaskSettingsStrategy {
        CompletionTaskSettingsStrategy(TaskType taskType) {
            super(taskType);
        }

        @Override
        public TaskSettings createTaskSettings(Map<String, Object> taskSettings, ConfigurationParseContext context) {
            return ImmutableEmptyTaskSettings.fromMap(taskSettings, context);
        }

        /**
         * Reasoning settings are supported by the cluster but not by the completion task,
         * so return the ImmutableEmptyTaskSettings instance.
         */
        @Override
        public Optional<TaskSettings> createTaskSettings(@Nullable Reasoning reasoning) {
            if (reasoning == null) {
                return Optional.of(ImmutableEmptyTaskSettings.INSTANCE);
            }

            return Optional.empty();
        }
    }
}
