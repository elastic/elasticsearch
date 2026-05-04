/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for VoyageAI service settings. Holds the fields that are common to every VoyageAI task type
 * ({@code model_id} and {@code rate_limit}) so concrete subclasses only need to manage their task-specific fields.
 */
public abstract class VoyageAIServiceSettings extends FilteredXContentObject implements ServiceSettings {

    /**
     * See <a href="https://docs.voyageai.com/docs/rate-limits">VoyageAI rate limits</a>
     */
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(2_000);

    /**
     * Extracts the {@link RateLimitSettings} from the supplied service-settings map, falling back to
     * {@link #DEFAULT_RATE_LIMIT_SETTINGS} when the map does not specify one.
     */
    protected static RateLimitSettings extractRateLimitSettings(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        return RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, VoyageAIService.NAME, context);
    }

    private final String modelId;
    private final RateLimitSettings rateLimitSettings;

    protected VoyageAIServiceSettings(String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = Objects.requireNonNull(modelId);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    protected VoyageAIServiceSettings(StreamInput in) throws IOException {
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

    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(ServiceFields.MODEL_ID, modelId);
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }
}
