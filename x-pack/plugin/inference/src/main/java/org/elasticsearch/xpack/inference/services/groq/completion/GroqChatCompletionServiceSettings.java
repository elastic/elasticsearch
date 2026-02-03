/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.groq.GroqRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.groq.GroqService;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

/**
 * Service settings for Groq chat completion models.
 * Groq reuses the OpenAI wire format, so this largely mirrors the OpenAI settings class
 * but applies Groq-specific defaults such as the base URL and rate limits documented at
 * https://console.groq.com/docs/rate-limits.
 */
public class GroqChatCompletionServiceSettings extends FilteredXContentObject implements ServiceSettings, GroqRateLimitServiceSettings {

    public static final String NAME = "groq_completion_service_settings";

    // The rate limit for dev tier depends on the model used. For example, the rate limit for the `openai/gpt-oss-20b` model is 1,000
    // requests per minute.
    // To find this information you need to access your account's limits https://console.groq.com/docs/rate-limits.
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1_000);

    public static GroqChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String modelId = ServiceUtils.extractRequiredString(
            map,
            ServiceFields.MODEL_ID,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        String organizationId = ServiceUtils.extractOptionalString(
            map,
            OpenAiServiceFields.ORGANIZATION,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        String url = ServiceUtils.extractOptionalString(map, ServiceFields.URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        URI uri = ServiceUtils.convertToUri(url, ServiceFields.URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            GroqService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new GroqChatCompletionServiceSettings(modelId, uri, organizationId, rateLimitSettings);
    }

    private final String modelId;
    @Nullable
    private final URI uri;
    @Nullable
    private final String organizationId;
    private final RateLimitSettings rateLimitSettings;

    public GroqChatCompletionServiceSettings(
        String modelId,
        @Nullable URI uri,
        @Nullable String organizationId,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.modelId = Objects.requireNonNull(modelId);
        this.uri = uri;
        this.organizationId = organizationId;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    GroqChatCompletionServiceSettings(
        String modelId,
        @Nullable String uri,
        @Nullable String organizationId,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this(modelId, ServiceUtils.createOptionalUri(uri), organizationId, rateLimitSettings);
    }

    public GroqChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.uri = ServiceUtils.createOptionalUri(in.readOptionalString());
        this.organizationId = in.readOptionalString();
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(ServiceFields.MODEL_ID, modelId);
        if (uri != null) {
            builder.field(ServiceFields.URL, uri.toString());
        }
        if (organizationId != null) {
            builder.field(OpenAiServiceFields.ORGANIZATION, organizationId);
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
        return GroqService.GROQ_INFERENCE_SERVICE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalString(uri != null ? uri.toString() : null);
        out.writeOptionalString(organizationId);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        GroqChatCompletionServiceSettings that = (GroqChatCompletionServiceSettings) object;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(organizationId, that.organizationId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, organizationId, rateLimitSettings);
    }
}
