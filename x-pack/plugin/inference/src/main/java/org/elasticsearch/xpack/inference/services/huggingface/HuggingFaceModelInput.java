/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.util.Map;

public class HuggingFaceModelInput {
    private final String inferenceEntityId;
    private final TaskType taskType;
    private final Map<String, Object> serviceSettings;
    @Nullable
    private final Map<String, Object> taskSettings;
    private final ChunkingSettings chunkingSettings;
    @Nullable
    private final Map<String, Object> secretSettings;
    private final String failureMessage;
    private final ConfigurationParseContext context;

    public HuggingFaceModelInput(Builder builder) {
        this.inferenceEntityId = builder.inferenceEntityId;
        this.taskType = builder.taskType;
        this.serviceSettings = builder.serviceSettings;
        this.taskSettings = builder.taskSettings;
        this.chunkingSettings = builder.chunkingSettings;
        this.secretSettings = builder.secretSettings;
        this.failureMessage = builder.failureMessage;
        this.context = builder.context;
    }

    public String getInferenceEntityId() {
        return inferenceEntityId;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public Map<String, Object> getServiceSettings() {
        return serviceSettings;
    }

    @Nullable
    public Map<String, Object> getTaskSettings() {
        return taskSettings;
    }

    public ChunkingSettings getChunkingSettings() {
        return chunkingSettings;
    }

    @Nullable
    public Map<String, Object> getSecretSettings() {
        return secretSettings;
    }

    public String getFailureMessage() {
        return failureMessage;
    }

    public ConfigurationParseContext getContext() {
        return context;
    }

    public static class Builder {
        private String inferenceEntityId;
        private TaskType taskType;
        private Map<String, Object> serviceSettings;
        @Nullable
        private Map<String, Object> taskSettings;
        private ChunkingSettings chunkingSettings;
        @Nullable
        Map<String, Object> secretSettings;
        private String failureMessage;
        private ConfigurationParseContext context;

        public Builder(
            String inferenceEntityId,
            TaskType taskType,
            Map<String, Object> serviceSettings,
            ChunkingSettings chunkingSettings,
            @Nullable Map<String, Object> secretSettings,
            String failureMessage,
            ConfigurationParseContext context
        ) {
            this.inferenceEntityId = inferenceEntityId;
            this.taskType = taskType;
            this.serviceSettings = serviceSettings;
            this.chunkingSettings = chunkingSettings;
            this.secretSettings = secretSettings;
            this.failureMessage = failureMessage;
            this.context = context;
        }

        public Builder withTaskSettings(Map<String, Object> taskSettings) {
            this.taskSettings = taskSettings;
            return this;
        }

        public HuggingFaceModelInput build() {
            return new HuggingFaceModelInput(this);
        }
    }
}
