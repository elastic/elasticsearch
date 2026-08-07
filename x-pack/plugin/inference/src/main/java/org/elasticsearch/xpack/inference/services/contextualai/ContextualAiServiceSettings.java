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
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiUtils.INFERENCE_CONTEXTUAL_AI_URL_SERVICE_SETTING_REMOVED;

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
        var rateLimitSettings = RateLimitSettings.of(serviceSettingsMap, defaultRateLimit, validationException, context);

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
            this(readModelId(in), new RateLimitSettings(in));
        }

        /**
         * Reads the model ID from the input stream, taking into account the transport version to maintain backward compatibility.
         * The URI setting used to be required to be sent by older versions of the coordinating node, but is no longer needed.
         * To maintain compatibility with mixed clusters, we need to continue reading it for older versions.
         * It is intentionally discarded for BWC.
         * @param in the input stream to read from
         * @return model ID read from the input stream
         * @throws IOException if there is an error reading from the input stream
         */
        private static String readModelId(StreamInput in) throws IOException {
            if (in.getTransportVersion().supports(INFERENCE_CONTEXTUAL_AI_URL_SERVICE_SETTING_REMOVED) == false) {
                in.readString();
            }
            return in.readString();
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
