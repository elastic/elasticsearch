/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

public class VoyageAICommonServiceSettings implements Writeable, ToXContentFragment {

    /**
     * See <a href="https://docs.voyageai.com/docs/rate-limits">VoyageAI rate limits</a>
     */
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(2_000);

    public static VoyageAICommonServiceSettings fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();

        var modelId = extractRequiredString(map, ServiceFields.MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, VoyageAIService.NAME, context);

        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }

        return new VoyageAICommonServiceSettings(modelId, rateLimitSettings);
    }

    private final String modelId;
    private final RateLimitSettings rateLimitSettings;

    public VoyageAICommonServiceSettings(String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = Objects.requireNonNull(modelId);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public VoyageAICommonServiceSettings(StreamInput in) throws IOException {
        this(in.readString(), new RateLimitSettings(in));
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    public String modelId() {
        return modelId;
    }

    /**
     * Returns a new {@link VoyageAICommonServiceSettings} merging the current settings with the mutable fields supplied via
     * {@code serviceSettings}. The {@code rate_limit} field is the only mutable field on the common settings; {@code model_id}
     * is treated as immutable and copied from {@code this}. Validation errors are accumulated into {@code validationException}
     * so callers can combine them with task-specific updates before calling
     * {@link ValidationException#throwIfValidationErrorsExist()}.
     */
    public VoyageAICommonServiceSettings updateCommonServiceSettings(
        Map<String, Object> serviceSettings,
        ValidationException validationException
    ) {
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            VoyageAIService.NAME,
            ConfigurationParseContext.REQUEST
        );

        return new VoyageAICommonServiceSettings(this.modelId, extractedRateLimitSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(ServiceFields.MODEL_ID, modelId);
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VoyageAICommonServiceSettings that = (VoyageAICommonServiceSettings) o;
        return Objects.equals(modelId, that.modelId) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, rateLimitSettings);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
