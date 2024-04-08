/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

/**
 * Defines the service settings for interacting with OpenAI's chat completion models.
 */
public class OpenAiChatCompletionServiceSettings implements ServiceSettings {

    public static final String NAME = "openai_completion_service_settings";

    public static OpenAiChatCompletionServiceSettings fromMap(Map<String, Object> map) {
        var commonSettings = OpenAiServiceSettings.fromMap(map);

        ValidationException validationException = new ValidationException();

        Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiChatCompletionServiceSettings(commonSettings, maxInputTokens);
    }

    private final OpenAiServiceSettings commonSettings;

    private final Integer maxInputTokens;

    public OpenAiChatCompletionServiceSettings(OpenAiServiceSettings commonSettings, @Nullable Integer maxInputTokens) {
        this.commonSettings = commonSettings;
        this.maxInputTokens = maxInputTokens;
    }

    public OpenAiChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new OpenAiServiceSettings(in);
        this.maxInputTokens = in.readOptionalVInt();
    }

    public String modelId() {
        return commonSettings.modelId();
    }

    public URI uri() {
        return commonSettings.uri();
    }

    public String organizationId() {
        return commonSettings.organizationId();
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        {
            commonSettings.toXContentFragment(builder);

            if (maxInputTokens != null) {
                builder.field(MAX_INPUT_TOKENS, maxInputTokens);
            }
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
        return TransportVersions.ML_COMPLETION_INFERENCE_SERVICE_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commonSettings.writeTo(out);
        out.writeOptionalVInt(maxInputTokens);
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        OpenAiChatCompletionServiceSettings that = (OpenAiChatCompletionServiceSettings) object;
        return Objects.equals(commonSettings, that.commonSettings) && Objects.equals(maxInputTokens, that.maxInputTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings, maxInputTokens);
    }
}
