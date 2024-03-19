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
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

/**
 * Defines the service settings for interacting with OpenAI's chat completion models.
 */
public class OpenAiChatCompletionServiceSettings implements ServiceSettings {

    public static final String NAME = "openai_completion_service_settings";

    static final String ORGANIZATION = "organization_id";

    public static OpenAiChatCompletionServiceSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String organizationId = extractOptionalString(map, ORGANIZATION, ModelConfigurations.SERVICE_SETTINGS, validationException);

        String url = extractOptionalString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        URI uri = convertToUri(url, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);

        Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiChatCompletionServiceSettings(modelId, uri, organizationId, maxInputTokens);
    }

    private final String modelId;

    private final URI uri;

    private final String organizationId;

    private final Integer maxInputTokens;

    public OpenAiChatCompletionServiceSettings(
        String modelId,
        @Nullable URI uri,
        @Nullable String organizationId,
        @Nullable Integer maxInputTokens
    ) {
        this.modelId = modelId;
        this.uri = uri;
        this.organizationId = organizationId;
        this.maxInputTokens = maxInputTokens;
    }

    OpenAiChatCompletionServiceSettings(
        String modelId,
        @Nullable String uri,
        @Nullable String organizationId,
        @Nullable Integer maxInputTokens
    ) {
        this(modelId, createOptionalUri(uri), organizationId, maxInputTokens);
    }

    public OpenAiChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.uri = createOptionalUri(in.readOptionalString());
        this.organizationId = in.readOptionalString();
        this.maxInputTokens = in.readOptionalVInt();
    }

    public String modelId() {
        return modelId;
    }

    public URI uri() {
        return uri;
    }

    public String organizationId() {
        return organizationId;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        {
            builder.field(MODEL_ID, modelId);

            if (uri != null) {
                builder.field(URL, uri.toString());
            }

            if (organizationId != null) {
                builder.field(ORGANIZATION, organizationId);
            }

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
        return TransportVersions.V_8_12_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalString(uri != null ? uri.toString() : null);
        out.writeOptionalString(organizationId);
        out.writeOptionalVInt(maxInputTokens);
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return (this::toXContent);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        OpenAiChatCompletionServiceSettings that = (OpenAiChatCompletionServiceSettings) object;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(organizationId, that.organizationId)
            && Objects.equals(maxInputTokens, that.maxInputTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, organizationId, maxInputTokens);
    }
}
