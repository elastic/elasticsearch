/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GoogleVertexAiChatCompletionTaskSettings implements TaskSettings {
    public static final String NAME = "google_vertex_ai_chatcompletion_task_settings";

    private final ThinkingConfig thinkingConfig;

    public static final GoogleVertexAiChatCompletionTaskSettings EMPTY_SETTINGS = new GoogleVertexAiChatCompletionTaskSettings();
    private static final ThinkingConfig EMPTY_THINKING_CONFIG = new ThinkingConfig();

    public GoogleVertexAiChatCompletionTaskSettings() {
        thinkingConfig = EMPTY_THINKING_CONFIG;
    }

    public GoogleVertexAiChatCompletionTaskSettings(ThinkingConfig thinkingConfig) {
        this.thinkingConfig = Objects.requireNonNullElse(thinkingConfig, EMPTY_THINKING_CONFIG);
    }

    public GoogleVertexAiChatCompletionTaskSettings(StreamInput in) throws IOException {
        thinkingConfig = new ThinkingConfig(in);
    }

    public static GoogleVertexAiChatCompletionTaskSettings fromMap(Map<String, Object> taskSettings) {
        ValidationException validationException = new ValidationException();

        // Extract optional thinkingConfig settings
        ThinkingConfig thinkingConfig = ThinkingConfig.fromMap(taskSettings, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new GoogleVertexAiChatCompletionTaskSettings(thinkingConfig);
    }

    public static GoogleVertexAiChatCompletionTaskSettings of(
        GoogleVertexAiChatCompletionTaskSettings originalTaskSettings,
        GoogleVertexAiChatCompletionTaskSettings newTaskSettings
    ) {
        ThinkingConfig thinkingConfig = newTaskSettings.thinkingConfig().isEmpty()
            ? originalTaskSettings.thinkingConfig()
            : newTaskSettings.thinkingConfig();
        return new GoogleVertexAiChatCompletionTaskSettings(thinkingConfig);
    }

    public ThinkingConfig thinkingConfig() {
        return thinkingConfig;
    }

    @Override
    public boolean isEmpty() {
        return thinkingConfig.isEmpty();
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        GoogleVertexAiChatCompletionTaskSettings newTaskSettings = GoogleVertexAiChatCompletionTaskSettings.fromMap(
            new HashMap<>(newSettings)
        );
        return GoogleVertexAiChatCompletionTaskSettings.of(this, newTaskSettings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.GEMINI_THINKING_BUDGET_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        thinkingConfig.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        thinkingConfig.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        GoogleVertexAiChatCompletionTaskSettings that = (GoogleVertexAiChatCompletionTaskSettings) o;
        return Objects.equals(thinkingConfig, that.thinkingConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(thinkingConfig);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
