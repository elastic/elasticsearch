/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.anthropic.AnthropicServiceFields.MAX_TOKENS;

public class GoogleVertexAiChatCompletionTaskSettings implements TaskSettings {
    public static final String NAME = "google_vertex_ai_chatcompletion_task_settings";

    private static final TransportVersion GEMINI_THINKING_BUDGET_ADDED = TransportVersion.fromName("gemini_thinking_budget_added");

    private final ThinkingConfig thinkingConfig;
    private final Integer maxTokens;

    public static final GoogleVertexAiChatCompletionTaskSettings EMPTY_SETTINGS = new GoogleVertexAiChatCompletionTaskSettings();
    private static final ThinkingConfig EMPTY_THINKING_CONFIG = new ThinkingConfig();

    public GoogleVertexAiChatCompletionTaskSettings() {
        this.thinkingConfig = EMPTY_THINKING_CONFIG;
        this.maxTokens = null;
    }

    public GoogleVertexAiChatCompletionTaskSettings(ThinkingConfig thinkingConfig, @Nullable Integer maxTokens) {
        this.thinkingConfig = Objects.requireNonNullElse(thinkingConfig, EMPTY_THINKING_CONFIG);
        this.maxTokens = maxTokens;
    }

    public GoogleVertexAiChatCompletionTaskSettings(StreamInput in) throws IOException {
        thinkingConfig = new ThinkingConfig(in);
        TransportVersion version = in.getTransportVersion();
        if (GoogleVertexAiUtils.supportsModelGarden(version)) {
            maxTokens = in.readOptionalVInt();
        } else {
            maxTokens = null;
        }
    }

    public static GoogleVertexAiChatCompletionTaskSettings fromMap(Map<String, Object> taskSettings) {
        ValidationException validationException = new ValidationException();

        // Extract optional thinkingConfig settings
        ThinkingConfig thinkingConfig = ThinkingConfig.fromMap(taskSettings, validationException);

        // Extract optional maxTokens setting
        Integer maxTokens = extractOptionalPositiveInteger(
            taskSettings,
            MAX_TOKENS,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new GoogleVertexAiChatCompletionTaskSettings(thinkingConfig, maxTokens);
    }

    public static GoogleVertexAiChatCompletionTaskSettings of(
        GoogleVertexAiChatCompletionTaskSettings originalTaskSettings,
        GoogleVertexAiChatCompletionTaskSettings newTaskSettings
    ) {
        ThinkingConfig thinkingConfig = newTaskSettings.thinkingConfig().isEmpty()
            ? originalTaskSettings.thinkingConfig()
            : newTaskSettings.thinkingConfig();

        Integer maxTokens = Objects.requireNonNullElse(newTaskSettings.maxTokens(), originalTaskSettings.maxTokens());
        return new GoogleVertexAiChatCompletionTaskSettings(thinkingConfig, maxTokens);
    }

    public ThinkingConfig thinkingConfig() {
        return thinkingConfig;
    }

    public Integer maxTokens() {
        return maxTokens;
    }

    @Override
    public boolean isEmpty() {
        return thinkingConfig.isEmpty() && Objects.isNull(maxTokens);
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
        return GEMINI_THINKING_BUDGET_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        thinkingConfig.writeTo(out);
        if (GoogleVertexAiUtils.supportsModelGarden(out.getTransportVersion())) {
            out.writeOptionalVInt(maxTokens);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        thinkingConfig.toXContent(builder, params);
        builder.field(MAX_TOKENS, maxTokens);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        GoogleVertexAiChatCompletionTaskSettings that = (GoogleVertexAiChatCompletionTaskSettings) o;
        return Objects.equals(thinkingConfig, that.thinkingConfig) && Objects.equals(maxTokens, that.maxTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(thinkingConfig, maxTokens);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
