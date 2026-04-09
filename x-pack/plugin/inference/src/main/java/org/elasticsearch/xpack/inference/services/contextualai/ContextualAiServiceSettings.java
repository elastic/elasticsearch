/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

/**
 * Base class for ServiceSettings of Contextual AI inference services.
 */
public abstract class ContextualAiServiceSettings extends FilteredXContentObject implements ServiceSettings {

    protected static CommonSettings fromMap(
        Map<String, Object> serviceSettingsMap,
        ConfigurationParseContext context,
        RateLimitSettings defaultRateLimit,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();

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

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }
        return new CommonSettings(modelId, rateLimitSettings);
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
        return new CommonSettings(this.commonSettings.modelId(), rateLimitSettings);
    }

    /**
     * Encapsulates the common settings for Contextual AI services, including the URI, model ID, and rate limit settings.
     * @param modelId the model ID to use when making requests to the Contextual AI service.
     * @param rateLimitSettings the rate limit settings for the Contextual AI service.
     */
    public record CommonSettings(String modelId, RateLimitSettings rateLimitSettings) {
        public CommonSettings {
            Objects.requireNonNull(modelId);
            Objects.requireNonNull(rateLimitSettings);
        }

        public CommonSettings(StreamInput in) throws IOException {
            this(in.readString(), new RateLimitSettings(in));
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

    protected ContextualAiServiceSettings(StreamInput in) throws IOException {
        this(new CommonSettings(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
        builder.field(MODEL_ID, commonSettings.modelId());
        commonSettings.rateLimitSettings().toXContent(builder, params);
        return builder;
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
