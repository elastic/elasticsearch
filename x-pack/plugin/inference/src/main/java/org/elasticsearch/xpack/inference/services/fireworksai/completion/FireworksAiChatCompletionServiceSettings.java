/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.completion;

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
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

public class FireworksAiChatCompletionServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        FireworksAiRateLimitServiceSettings {

    public static final String NAME = "fireworksai_completion_service_settings";

    // FireworksAI rate limits: https://docs.fireworks.ai/guides/quotas_usage/rate-limits
    // Default rate limit for the serverless API is 6000 RPM
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(6_000);

    public static FireworksAiChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String modelId = ServiceUtils.extractRequiredString(
            map,
            ServiceFields.MODEL_ID,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        String url = ServiceUtils.extractOptionalString(map, ServiceFields.URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        URI uri = ServiceUtils.convertToUri(url, ServiceFields.URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            FireworksAiService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new FireworksAiChatCompletionServiceSettings(modelId, uri, rateLimitSettings);
    }

    private final String modelId;
    @Nullable
    private final URI uri;
    private final RateLimitSettings rateLimitSettings;

    public FireworksAiChatCompletionServiceSettings(String modelId, @Nullable URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = Objects.requireNonNull(modelId);
        this.uri = uri;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public FireworksAiChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.uri = ServiceUtils.createOptionalUri(in.readOptionalString());
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

    public URI uri() {
        return uri;
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
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return FireworksAiService.INFERENCE_API_FIREWORKS_AI_CHAT_COMPLETION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalString(uri != null ? uri.toString() : null);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        FireworksAiChatCompletionServiceSettings that = (FireworksAiChatCompletionServiceSettings) object;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, rateLimitSettings);
    }
}
