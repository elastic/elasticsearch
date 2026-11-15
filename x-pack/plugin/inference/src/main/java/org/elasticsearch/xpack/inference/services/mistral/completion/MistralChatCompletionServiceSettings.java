/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.SettingsScope.SERVICE_SETTINGS;
import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.INFERENCE_API_MISTRAL_URL_ADDED;
import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.MODEL_FIELD;

/**
 * Represents the settings for the Mistral chat completion service.
 * This class encapsulates the model ID and rate limit settings for the Mistral chat completion service.
 */
public class MistralChatCompletionServiceSettings extends FilteredXContentObject implements ServiceSettings {
    public static final String NAME = "mistral_completions_service_settings";
    private static final TransportVersion ML_INFERENCE_MISTRAL_CHAT_COMPLETION_ADDED = TransportVersion.fromName(
        "ml_inference_mistral_chat_completion_added"
    );

    private final String modelId;
    @Nullable
    private final URI url;
    private final RateLimitSettings rateLimitSettings;

    // default for Mistral is 5 requests / sec
    // setting this to 240 (4 requests / sec) is a sane default for us
    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(240);

    public static MistralChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var modelId = extractRequiredString(map, MODEL_FIELD, SERVICE_SETTINGS, validationException);
        var url = extractOptionalUri(map, URL, validationException);
        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, context);

        validationException.throwIfValidationErrorsExist();

        return new MistralChatCompletionServiceSettings(modelId, url, rateLimitSettings);
    }

    @Override
    public MistralChatCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            ConfigurationParseContext.REQUEST
        );

        validationException.throwIfValidationErrorsExist();

        return new MistralChatCompletionServiceSettings(this.modelId, this.url, extractedRateLimitSettings);
    }

    public MistralChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.url = in.getTransportVersion().supports(INFERENCE_API_MISTRAL_URL_ADDED) ? createOptionalUri(in.readOptionalString()) : null;
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    public MistralChatCompletionServiceSettings(String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        this(modelId, null, rateLimitSettings);
    }

    public MistralChatCompletionServiceSettings(String modelId, @Nullable URI url, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = modelId;
        this.url = url;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return ML_INFERENCE_MISTRAL_CHAT_COMPLETION_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(ML_INFERENCE_MISTRAL_CHAT_COMPLETION_ADDED);
    }

    @Override
    public String modelId() {
        return this.modelId;
    }

    /**
     * The custom endpoint url, or {@code null} when the default Mistral endpoint should be used.
     */
    @Nullable
    public URI url() {
        return this.url;
    }

    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        if (out.getTransportVersion().supports(INFERENCE_API_MISTRAL_URL_ADDED)) {
            out.writeOptionalString(url != null ? url.toString() : null);
        }
        rateLimitSettings.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        this.toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_FIELD, this.modelId);
        if (url != null) {
            builder.field(URL, url.toString());
        }

        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MistralChatCompletionServiceSettings that = (MistralChatCompletionServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(url, that.url)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, url, rateLimitSettings);
    }

}
