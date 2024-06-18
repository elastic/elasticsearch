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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

public class AnthropicChatCompletionTaskSettings implements TaskSettings {

    public static final String NAME = "anthropic_completion_task_settings";

    public static AnthropicChatCompletionTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        @SuppressWarnings("unchecked")
        Map<String, Object> optionalSettings = (Map<String, Object>) removeAsType(map, "optional_settings", Map.class);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AnthropicChatCompletionTaskSettings(optionalSettings);
    }

    private final Map<String, Object> optionalSettings;

    public AnthropicChatCompletionTaskSettings(@Nullable Map<String, Object> optionalSettings) {
        this.optionalSettings = optionalSettings;
    }

    public AnthropicChatCompletionTaskSettings(StreamInput in) throws IOException {
        this.optionalSettings = in.readGenericMap();
    }

    public static AnthropicChatCompletionTaskSettings of(
        AnthropicChatCompletionTaskSettings originalSettings,
        AnthropicChatCompletionRequestTaskSettings requestSettings
    ) {
        var mergedTaskSettings = new HashMap<>(originalSettings.optionalSettings);
        mergedTaskSettings.putAll(Objects.requireNonNullElse(requestSettings.optionalSettings(), Map.of()));
        return new AnthropicChatCompletionTaskSettings(mergedTaskSettings);
    }

    public Map<String, Object> optionalSettings() {
        return optionalSettings;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (optionalSettings != null) {
            builder.field("optional_settings", optionalSettings);
        }

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
        out.writeGenericMap(optionalSettings);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        AnthropicChatCompletionTaskSettings that = (AnthropicChatCompletionTaskSettings) object;
        return Objects.equals(optionalSettings, that.optionalSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(optionalSettings);
    }
}
