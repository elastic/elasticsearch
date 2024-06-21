/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredPositiveInteger;
import static org.elasticsearch.xpack.inference.services.anthropic.AnthropicServiceFields.MAX_TOKENS;

public class AnthropicChatCompletionTaskSettings implements TaskSettings {

    public static final String NAME = "anthropic_completion_task_settings";

    public static AnthropicChatCompletionTaskSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return switch (context) {
            case REQUEST -> fromRequestMap(map);
            case PERSISTENT -> fromPersistedMap(map);
        };
    }

    private static AnthropicChatCompletionTaskSettings fromRequestMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        var commonFields = fromMap(map, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AnthropicChatCompletionTaskSettings(commonFields);
    }

    private static AnthropicChatCompletionTaskSettings fromPersistedMap(Map<String, Object> map) {
        var commonFields = fromMap(map, new ValidationException());

        return new AnthropicChatCompletionTaskSettings(commonFields);
    }

    private record CommonFields(int maxTokens) {}

    private static CommonFields fromMap(Map<String, Object> map, ValidationException validationException) {
        Integer maxTokens = extractRequiredPositiveInteger(map, MAX_TOKENS, ModelConfigurations.TASK_SETTINGS, validationException);
        return new CommonFields(Objects.requireNonNullElse(maxTokens, -1));
    }

    public static AnthropicChatCompletionTaskSettings of(
        AnthropicChatCompletionTaskSettings originalSettings,
        AnthropicChatCompletionRequestTaskSettings requestSettings
    ) {
        return new AnthropicChatCompletionTaskSettings(Objects.requireNonNullElse(requestSettings.maxTokens(), originalSettings.maxTokens));
    }

    private final int maxTokens;

    public AnthropicChatCompletionTaskSettings(int maxTokens) {
        this.maxTokens = maxTokens;
    }

    public AnthropicChatCompletionTaskSettings(StreamInput in) throws IOException {
        this.maxTokens = in.readVInt();
    }

    private AnthropicChatCompletionTaskSettings(CommonFields commonFields) {
        this(commonFields.maxTokens);
    }

    public int maxTokens() {
        return maxTokens;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field("maxTokens", maxTokens);

        builder.endObject();

        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_ANTHROPIC_INTEGRATION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(maxTokens);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        AnthropicChatCompletionTaskSettings that = (AnthropicChatCompletionTaskSettings) object;
        return Objects.equals(maxTokens, that.maxTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxTokens);
    }
}
