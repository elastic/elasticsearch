/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

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

/**
 * Base class for VoyageAI service settings. Holds the fields that are common to every VoyageAI task type
 * ({@code model_id} and {@code rate_limit}) so concrete subclasses only need to manage their task-specific fields.
 */
public class VoyageAIServiceSettings extends FilteredXContentObject implements ServiceSettings, VoyageAIRateLimitServiceSettings {

    public static final String NAME = "voyageai_service_settings";

    private static final TransportVersion VOYAGE_AI_INTEGRATION_ADDED = TransportVersion.fromName("voyage_ai_integration_added");

    /**
     * See <a href="https://docs.voyageai.com/docs/rate-limits">VoyageAI rate limits</a>
     */
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(2_000);

    @Nullable
    public static VoyageAIServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var modelId = extractRequiredString(map, ServiceFields.MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var rateLimitSettings = extractRateLimitSettings(map, context, validationException);

        validationException.throwIfValidationErrorsExist();

        return new VoyageAIServiceSettings(modelId, rateLimitSettings);
    }

    /**
     * Extracts the {@link RateLimitSettings} from the supplied service-settings map, falling back to
     * {@link #DEFAULT_RATE_LIMIT_SETTINGS} when the map does not specify one.
     */
    protected static RateLimitSettings extractRateLimitSettings(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        return RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, context);
    }

    private final String modelId;
    private final RateLimitSettings rateLimitSettings;

    public VoyageAIServiceSettings(String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = Objects.requireNonNull(modelId);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public VoyageAIServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public String modelId() {
        return modelId;
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(ServiceFields.MODEL_ID, modelId);
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return VOYAGE_AI_INTEGRATION_ADDED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VoyageAIServiceSettings that = (VoyageAIServiceSettings) o;
        return Objects.equals(modelId, that.modelId) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, rateLimitSettings);
    }
}
