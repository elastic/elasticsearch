/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalDoubleInRange;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MAX_NEW_TOKENS_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MAX_TEMPERATURE_TOP_P_TOP_K_VALUE;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MIN_TEMPERATURE_TOP_P_TOP_K_VALUE;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TOP_K_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TOP_P_FIELD;

public class AmazonBedrockChatCompletionTaskSettings implements TaskSettings {
    public static final String NAME = "amazon_bedrock_chat_completion_task_settings";

    public static final AmazonBedrockChatCompletionRequestTaskSettings EMPTY_SETTINGS = new AmazonBedrockChatCompletionRequestTaskSettings(
        null,
        null,
        null,
        null
    );

    public static AmazonBedrockChatCompletionTaskSettings fromMap(Map<String, Object> settings) {
        ValidationException validationException = new ValidationException();

        Double temperature = extractOptionalDoubleInRange(
            settings,
            TEMPERATURE_FIELD,
            MIN_TEMPERATURE_TOP_P_TOP_K_VALUE,
            MAX_TEMPERATURE_TOP_P_TOP_K_VALUE,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        Double topP = extractOptionalDoubleInRange(
            settings,
            TOP_P_FIELD,
            MIN_TEMPERATURE_TOP_P_TOP_K_VALUE,
            MAX_TEMPERATURE_TOP_P_TOP_K_VALUE,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        Double topK = extractOptionalDoubleInRange(
            settings,
            TOP_K_FIELD,
            MIN_TEMPERATURE_TOP_P_TOP_K_VALUE,
            MAX_TEMPERATURE_TOP_P_TOP_K_VALUE,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        Integer maxNewTokens = extractOptionalPositiveInteger(
            settings,
            MAX_NEW_TOKENS_FIELD,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AmazonBedrockChatCompletionTaskSettings(temperature, topP, topK, maxNewTokens);
    }

    public static AmazonBedrockChatCompletionTaskSettings of(
        AmazonBedrockChatCompletionTaskSettings originalSettings,
        AmazonBedrockChatCompletionRequestTaskSettings requestSettings
    ) {
        var temperature = requestSettings.temperature() == null ? originalSettings.temperature() : requestSettings.temperature();
        var topP = requestSettings.topP() == null ? originalSettings.topP() : requestSettings.topP();
        var topK = requestSettings.topK() == null ? originalSettings.topK() : requestSettings.topK();
        var maxNewTokens = requestSettings.maxNewTokens() == null ? originalSettings.maxNewTokens() : requestSettings.maxNewTokens();

        return new AmazonBedrockChatCompletionTaskSettings(temperature, topP, topK, maxNewTokens);
    }

    private final Double temperature;
    private final Double topP;
    private final Double topK;
    private final Integer maxNewTokens;

    public AmazonBedrockChatCompletionTaskSettings(
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Double topK,
        @Nullable Integer maxNewTokens
    ) {
        this.temperature = temperature;
        this.topP = topP;
        this.topK = topK;
        this.maxNewTokens = maxNewTokens;
    }

    public AmazonBedrockChatCompletionTaskSettings(StreamInput in) throws IOException {
        this.temperature = in.readOptionalDouble();
        this.topP = in.readOptionalDouble();
        this.topK = in.readOptionalDouble();
        this.maxNewTokens = in.readOptionalVInt();
    }

    public Double temperature() {
        return temperature;
    }

    public Double topP() {
        return topP;
    }

    public Double topK() {
        return topK;
    }

    public Integer maxNewTokens() {
        return maxNewTokens;
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
        out.writeOptionalDouble(temperature);
        out.writeOptionalDouble(topP);
        out.writeOptionalDouble(topK);
        out.writeOptionalVInt(maxNewTokens);
    }

    @Override
    public boolean isEmpty() {
        return temperature == null && topP == null && topK == null && maxNewTokens == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (temperature != null) {
                builder.field(TEMPERATURE_FIELD, temperature);
            }
            if (topP != null) {
                builder.field(TOP_P_FIELD, topP);
            }
            if (topK != null) {
                builder.field(TOP_K_FIELD, topK);
            }
            if (maxNewTokens != null) {
                builder.field(MAX_NEW_TOKENS_FIELD, maxNewTokens);
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AmazonBedrockChatCompletionTaskSettings that = (AmazonBedrockChatCompletionTaskSettings) o;
        return Objects.equals(temperature, that.temperature)
            && Objects.equals(topP, that.topP)
            && Objects.equals(topK, that.topK)
            && Objects.equals(maxNewTokens, that.maxNewTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(temperature, topP, topK, maxNewTokens);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        AmazonBedrockChatCompletionRequestTaskSettings requestSettings = AmazonBedrockChatCompletionRequestTaskSettings.fromMap(
            new HashMap<>(newSettings)
        );
        return of(this, requestSettings);
    }
}
