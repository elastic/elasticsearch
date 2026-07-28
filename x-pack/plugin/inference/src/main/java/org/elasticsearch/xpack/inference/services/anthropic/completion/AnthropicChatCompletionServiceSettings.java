/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

/**
 * Defines the service settings for interacting with Anthropic's chat completion models.
 */
public class AnthropicChatCompletionServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        AnthropicRateLimitServiceSettings {

    public static final String NAME = "anthropic_completion_service_settings";

    static final TransportVersion ANTHROPIC_COMPLETION_URL_ADDED = TransportVersion.fromName(
        "inference_api_anthropic_completion_url_added"
    );

    // The rate limit for build tier 1 is 50 request per minute
    // Details are here https://docs.anthropic.com/en/api/rate-limits
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(50);

    public static AnthropicChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var url = extractOptionalUri(map, URL, validationException);
        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, context);

        validationException.throwIfValidationErrorsExist();

        return new AnthropicChatCompletionServiceSettings(modelId, url, rateLimitSettings);
    }

    private final String modelId;
    @Nullable
    private final URI url;
    private final RateLimitSettings rateLimitSettings;

    public AnthropicChatCompletionServiceSettings(String modelId, @Nullable URI url, @Nullable RateLimitSettings ratelimitSettings) {
        this.modelId = Objects.requireNonNull(modelId);
        this.url = url;
        this.rateLimitSettings = Objects.requireNonNullElse(ratelimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public AnthropicChatCompletionServiceSettings(String modelId, @Nullable RateLimitSettings ratelimitSettings) {
        this(modelId, null, ratelimitSettings);
    }

    public AnthropicChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.url = in.getTransportVersion().supports(ANTHROPIC_COMPLETION_URL_ADDED) ? createOptionalUri(in.readOptionalString()) : null;
        rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Nullable
    public URI url() {
        return url;
    }

    @Override
    public AnthropicChatCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            ConfigurationParseContext.REQUEST
        );

        validationException.throwIfValidationErrorsExist();

        return new AnthropicChatCompletionServiceSettings(this.modelId, this.url, extractedRateLimitSettings);
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
        if (url != null) {
            builder.field(URL, url.toString());
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
        if (out.getTransportVersion().supports(ANTHROPIC_COMPLETION_URL_ADDED)) {
            out.writeOptionalString(url != null ? url.toString() : null);
        }
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        AnthropicChatCompletionServiceSettings that = (AnthropicChatCompletionServiceSettings) object;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(url, that.url)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, url, rateLimitSettings);
    }
}
