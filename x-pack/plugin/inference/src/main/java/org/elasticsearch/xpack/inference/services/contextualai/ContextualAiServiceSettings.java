/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

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
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiUtils.ML_INFERENCE_CONTEXTUAL_AI_ADDED;

public abstract class ContextualAiServiceSettings extends FilteredXContentObject implements ServiceSettings {

    protected static CommonSettings fromMap(
        Map<String, Object> serviceSettingsMap,
        ConfigurationParseContext context,
        URI defaultUri,
        RateLimitSettings defaultRateLimit,
        ValidationException validationException
    ) {
        var uri = Objects.requireNonNullElse(extractOptionalUri(serviceSettingsMap, URL, validationException), defaultUri);
        var modelId = extractRequiredString(
            serviceSettingsMap,
            ServiceFields.MODEL_ID,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var rateLimitSettings = RateLimitSettings.of(
            serviceSettingsMap,
            defaultRateLimit,
            validationException,
            ContextualAiService.NAME,
            context
        );
        return new CommonSettings(uri, modelId, rateLimitSettings);
    }

    protected ContextualAiServiceSettings(CommonSettings commonSettings) {
        this.commonSettings = Objects.requireNonNull(commonSettings);
    }

    protected CommonSettings updateCommonSettings(Map<String, Object> serviceSettings, ValidationException validationException) {
        var rateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.commonSettings.rateLimitSettings(),
            validationException,
            ContextualAiService.NAME,
            ConfigurationParseContext.REQUEST
        );
        return new CommonSettings(this.commonSettings.uri(), this.commonSettings.modelId(), rateLimitSettings);
    }

    /**
     * Encapsulates the common settings for Contextual AI services, including the URI, model ID, and rate limit settings.
     * @param uri the URI to use when making requests to the Contextual AI service.
     * @param modelId the model ID to use when making requests to the Contextual AI service.
     * @param rateLimitSettings the rate limit settings for the Contextual AI service.
     */
    public record CommonSettings(@Nullable URI uri, @Nullable String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        public CommonSettings(StreamInput in) throws IOException {
            this(ServiceUtils.createUri(in.readString()), in.readString(), new RateLimitSettings(in));
        }
    }

    private final CommonSettings commonSettings;

    @Override
    public String modelId() {
        return commonSettings.modelId();
    }

    public RateLimitSettings rateLimitSettings() {
        return commonSettings.rateLimitSettings();
    }

    public URI uri() {
        return commonSettings.uri();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return ML_INFERENCE_CONTEXTUAL_AI_ADDED;
    }

    protected ContextualAiServiceSettings(StreamInput in) throws IOException {
        this(new CommonSettings(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(commonSettings.uri().toString());
        out.writeString(commonSettings.modelId());
        commonSettings.rateLimitSettings().writeTo(out);
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
        builder.field(URL, commonSettings.uri().toString());
        builder.field(MODEL_ID, commonSettings.modelId());
        commonSettings.rateLimitSettings().toXContent(builder, params);
        return builder;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return ContextualAiUtils.supportsContextualAi(version);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ContextualAiServiceSettings that = (ContextualAiServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(commonSettings);
    }
}
