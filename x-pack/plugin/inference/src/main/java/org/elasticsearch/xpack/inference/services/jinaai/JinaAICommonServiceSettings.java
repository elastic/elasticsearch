/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

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
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIService.JINA_AI_EMBEDDING_REFACTOR;

public class JinaAICommonServiceSettings extends FilteredXContentObject implements ServiceSettings, JinaAIRateLimitServiceSettings {

    public static final String NAME = "jinaai_service_settings";
    // See https://jina.ai/contact-sales/#rate-limit
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(2_000);

    @Nullable
    public static JinaAICommonServiceSettings fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();

        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, context);

        var modelId = extractRequiredString(map, ServiceFields.MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            // new validation error occurred
            return null;
        }
        return new JinaAICommonServiceSettings(modelId, rateLimitSettings);
    }

    private final String modelId;
    private final RateLimitSettings rateLimitSettings;

    public JinaAICommonServiceSettings(String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = Objects.requireNonNull(modelId);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public JinaAICommonServiceSettings(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(JINA_AI_EMBEDDING_REFACTOR) == false) {
            // URI is no longer part of service settings since it's only used for testing
            in.readOptionalString();
            // ModelID was incorrectly being serialized as optional
            modelId = in.readOptionalString();
        } else {
            modelId = in.readString();
        }
        rateLimitSettings = new RateLimitSettings(in);
    }

    @Nullable
    public JinaAICommonServiceSettings updateCommonServiceSettings(
        Map<String, Object> serviceSettings,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();

        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            ConfigurationParseContext.REQUEST
        );
        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            // new validation error occurred
            return null;
        }

        return new JinaAICommonServiceSettings(this.modelId, extractedRateLimitSettings);
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
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragment(builder, params);

        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        return toXContentFragmentOfExposedFields(builder, params);
    }

    @Override
    public XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(ServiceFields.MODEL_ID, modelId);
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(JINA_AI_EMBEDDING_REFACTOR) == false) {
            // URI is no longer part of service settings since it's only used for testing
            out.writeOptionalString(null);
            // ModelID was incorrectly being serialized as optional
            out.writeOptionalString(modelId);
        } else {
            out.writeString(modelId);
        }
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JinaAICommonServiceSettings that = (JinaAICommonServiceSettings) o;
        return Objects.equals(modelId, that.modelId) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, rateLimitSettings);
    }
}
