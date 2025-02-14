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
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.nonNullOrDefault;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;
import static org.elasticsearch.xpack.inference.services.anthropic.AnthropicServiceFields.MAX_TOKENS;
import static org.elasticsearch.xpack.inference.services.anthropic.AnthropicServiceFields.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.anthropic.AnthropicServiceFields.TOP_K_FIELD;
import static org.elasticsearch.xpack.inference.services.anthropic.AnthropicServiceFields.TOP_P_FIELD;

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

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return fromRequestMap(new HashMap<>(newSettings));
    }

    private record CommonFields(int maxTokens, Double temperature, Double topP, Integer topK) {}

    private static CommonFields fromMap(Map<String, Object> map, ValidationException validationException) {
        Integer maxTokens = extractRequiredPositiveInteger(map, MAX_TOKENS, ModelConfigurations.TASK_SETTINGS, validationException);

        // At the time of writing the allowed values for the temperature field are -1, and range 0-1.
        // I'm intentionally not validating the values here, we'll let Anthropic return an error when we send it instead.
        Double temperature = removeAsType(map, TEMPERATURE_FIELD, Double.class);

        // I'm intentionally not validating these so that Anthropic will return an error if they aren't in the correct range
        Double topP = removeAsType(map, TOP_P_FIELD, Double.class);
        Integer topK = removeAsType(map, TOP_K_FIELD, Integer.class);

        return new CommonFields(Objects.requireNonNullElse(maxTokens, -1), temperature, topP, topK);
    }

    public static AnthropicChatCompletionTaskSettings of(
        AnthropicChatCompletionTaskSettings originalSettings,
        AnthropicChatCompletionRequestTaskSettings requestSettings
    ) {
        return new AnthropicChatCompletionTaskSettings(
            Objects.requireNonNullElse(requestSettings.maxTokens(), originalSettings.maxTokens),
            nonNullOrDefault(requestSettings.temperature(), originalSettings.temperature),
            nonNullOrDefault(requestSettings.topP(), originalSettings.topP),
            nonNullOrDefault(requestSettings.topK(), originalSettings.topK)
        );
    }

    private final int maxTokens;
    private final Double temperature;
    private final Double topP;
    private final Integer topK;

    public AnthropicChatCompletionTaskSettings(int maxTokens, @Nullable Double temperature, @Nullable Double topP, @Nullable Integer topK) {
        this.maxTokens = maxTokens;
        this.temperature = temperature;
        this.topP = topP;
        this.topK = topK;
    }

    public AnthropicChatCompletionTaskSettings(StreamInput in) throws IOException {
        this.maxTokens = in.readVInt();
        this.temperature = in.readOptionalDouble();
        this.topP = in.readOptionalDouble();
        this.topK = in.readOptionalInt();
    }

    private AnthropicChatCompletionTaskSettings(CommonFields commonFields) {
        this(commonFields.maxTokens, commonFields.temperature, commonFields.topP, commonFields.topK);
    }

    public int maxTokens() {
        return maxTokens;
    }

    public Double temperature() {
        return temperature;
    }

    public Double topP() {
        return topP;
    }

    public Integer topK() {
        return topK;
    }

    @Override
    public boolean isEmpty() {
        return false; // maxTokens is non-optional
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MAX_TOKENS, maxTokens);

        if (temperature != null) {
            builder.field(TEMPERATURE_FIELD, temperature);
        }

        if (topP != null) {
            builder.field(TOP_P_FIELD, topP);
        }

        if (topK != null) {
            builder.field(TOP_P_FIELD, topK);
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
        return TransportVersions.V_8_15_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(maxTokens);
        out.writeOptionalDouble(temperature);
        out.writeOptionalDouble(topP);
        out.writeOptionalInt(topK);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        AnthropicChatCompletionTaskSettings that = (AnthropicChatCompletionTaskSettings) object;
        return Objects.equals(maxTokens, that.maxTokens)
            && Objects.equals(temperature, that.temperature)
            && Objects.equals(topP, that.topP)
            && Objects.equals(topK, that.topK);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxTokens, temperature, topP, topK);
    }
}
