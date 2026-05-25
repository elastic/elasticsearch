/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openai.OpenAiRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.ORGANIZATION;

/**
 * Defines the service settings for interacting with OpenAI's chat completion models.
 */
public class OpenAiChatCompletionServiceSettings extends FilteredXContentObject implements ServiceSettings, OpenAiRateLimitServiceSettings {

    public static final String NAME = "openai_completion_service_settings";

    // The rate limit for usage tier 1 is 500 request per minute for most of the completion models
    // To find this information you need to access your account's limits https://platform.openai.com/account/limits
    // 500 requests per minute
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(500);

    public static OpenAiChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var organizationId = extractOptionalString(map, ORGANIZATION, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var uri = extractOptionalUri(map, URL, validationException);
        var maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, context);

        validationException.throwIfValidationErrorsExist();

        return new OpenAiChatCompletionServiceSettings(modelId, uri, organizationId, maxInputTokens, rateLimitSettings);
    }

    @Override
    public OpenAiChatCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var extractedOrganizationId = extractOptionalString(
            serviceSettings,
            ORGANIZATION,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var extractedMaxInputTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            ConfigurationParseContext.REQUEST
        );

        validationException.throwIfValidationErrorsExist();

        return new OpenAiChatCompletionServiceSettings(
            this.modelId,
            this.uri,
            extractedOrganizationId != null ? extractedOrganizationId : this.organizationId,
            extractedMaxInputTokens != null ? extractedMaxInputTokens : this.maxInputTokens,
            extractedRateLimitSettings
        );
    }

    private final String modelId;
    private final URI uri;
    private final String organizationId;
    private final Integer maxInputTokens;
    private final RateLimitSettings rateLimitSettings;

    public OpenAiChatCompletionServiceSettings(
        String modelId,
        @Nullable URI uri,
        @Nullable String organizationId,
        @Nullable Integer maxInputTokens,
        @Nullable RateLimitSettings ratelimitSettings
    ) {
        this.modelId = modelId;
        this.uri = uri;
        this.organizationId = organizationId;
        this.maxInputTokens = maxInputTokens;
        this.rateLimitSettings = Objects.requireNonNullElse(ratelimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    OpenAiChatCompletionServiceSettings(
        String modelId,
        @Nullable String uri,
        @Nullable String organizationId,
        @Nullable Integer maxInputTokens,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this(modelId, createOptionalUri(uri), organizationId, maxInputTokens, rateLimitSettings);
    }

    public OpenAiChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.uri = createOptionalUri(in.readOptionalString());
        this.organizationId = in.readOptionalString();
        this.maxInputTokens = in.readOptionalVInt();
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public URI uri() {
        return uri;
    }

    @Override
    public String organizationId() {
        return organizationId;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragmentOfExposedFields(builder, params);

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
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
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalString(uri != null ? uri.toString() : null);
        out.writeOptionalString(organizationId);
        out.writeOptionalVInt(maxInputTokens);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        OpenAiChatCompletionServiceSettings that = (OpenAiChatCompletionServiceSettings) object;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(organizationId, that.organizationId)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, organizationId, maxInputTokens, rateLimitSettings);
    }
}
