/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalDoubleInRange;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.DO_SAMPLE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.MAX_NEW_TOKENS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TOP_P_FIELD;

public class AzureAiStudioChatCompletionTaskSettings implements TaskSettings {
    public static final String NAME = "azure_ai_studio_chat_completion_task_settings";
    public static final Integer DEFAULT_MAX_NEW_TOKENS = 64;

    public static AzureAiStudioChatCompletionTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        var temperature = extractOptionalDoubleInRange(
            map,
            TEMPERATURE_FIELD,
            AzureAiStudioConstants.MIN_TEMPERATURE_TOP_P,
            AzureAiStudioConstants.MAX_TEMPERATURE_TOP_P,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        var topP = extractOptionalDoubleInRange(
            map,
            TOP_P_FIELD,
            AzureAiStudioConstants.MIN_TEMPERATURE_TOP_P,
            AzureAiStudioConstants.MAX_TEMPERATURE_TOP_P,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        var doSample = extractOptionalBoolean(map, DO_SAMPLE_FIELD, validationException);
        var maxNewTokens = extractOptionalPositiveInteger(
            map,
            MAX_NEW_TOKENS_FIELD,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureAiStudioChatCompletionTaskSettings(temperature, topP, doSample, maxNewTokens);
    }

    /**
     * Creates a new {@link AzureOpenAiEmbeddingsTaskSettings} object by overriding the values in originalSettings with the ones
     * passed in via requestSettings if the fields are not null.
     * @param originalSettings the original {@link AzureOpenAiEmbeddingsTaskSettings} from the inference entity configuration from storage
     * @param requestSettings the {@link AzureOpenAiEmbeddingsTaskSettings} from the request
     * @return a new {@link AzureOpenAiEmbeddingsTaskSettings}
     */
    public static AzureAiStudioChatCompletionTaskSettings of(
        AzureAiStudioChatCompletionTaskSettings originalSettings,
        AzureAiStudioChatCompletionRequestTaskSettings requestSettings
    ) {

        var temperature = requestSettings.temperature() == null ? originalSettings.temperature() : requestSettings.temperature();
        var topP = requestSettings.topP() == null ? originalSettings.topP() : requestSettings.topP();
        var doSample = requestSettings.doSample() == null ? originalSettings.doSample() : requestSettings.doSample();
        var maxNewTokens = requestSettings.maxNewTokens() == null ? originalSettings.maxNewTokens() : requestSettings.maxNewTokens();

        return new AzureAiStudioChatCompletionTaskSettings(temperature, topP, doSample, maxNewTokens);
    }

    public AzureAiStudioChatCompletionTaskSettings(
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Boolean doSample,
        @Nullable Integer maxNewTokens
    ) {

        this.temperature = temperature;
        this.topP = topP;
        this.doSample = doSample;
        this.maxNewTokens = maxNewTokens;
    }

    public AzureAiStudioChatCompletionTaskSettings(StreamInput in) throws IOException {
        this.temperature = in.readOptionalDouble();
        this.topP = in.readOptionalDouble();
        this.doSample = in.readOptionalBoolean();
        this.maxNewTokens = in.readOptionalInt();
    }

    private final Double temperature;
    private final Double topP;
    private final Boolean doSample;
    private final Integer maxNewTokens;

    public Double temperature() {
        return temperature;
    }

    public Double topP() {
        return topP;
    }

    public Boolean doSample() {
        return doSample;
    }

    public Integer maxNewTokens() {
        return maxNewTokens;
    }

    public boolean areAnyParametersAvailable() {
        return temperature != null && topP != null && doSample != null && maxNewTokens != null;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_14_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalDouble(temperature);
        out.writeOptionalDouble(topP);
        out.writeOptionalBoolean(doSample);
        out.writeOptionalInt(maxNewTokens);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (temperature != null) {
            builder.field(TEMPERATURE_FIELD, temperature);
        }
        if (topP != null) {
            builder.field(TOP_P_FIELD, topP);
        }
        if (doSample != null) {
            builder.field(DO_SAMPLE_FIELD, doSample);
        }
        if (maxNewTokens != null) {
            builder.field(MAX_NEW_TOKENS_FIELD, maxNewTokens);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureAiStudioChatCompletionTaskSettings that = (AzureAiStudioChatCompletionTaskSettings) o;
        return Objects.equals(temperature, that.temperature)
            && Objects.equals(topP, that.topP)
            && Objects.equals(doSample, that.doSample)
            && Objects.equals(maxNewTokens, that.maxNewTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(temperature, topP, doSample, maxNewTokens);
    }

}
