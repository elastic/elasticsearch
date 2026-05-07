/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

public class VoyageAIRerankServiceSettings extends VoyageAIServiceSettings {
    public static final String NAME = "voyageai_rerank_service_settings";

    private static final TransportVersion VOYAGE_AI_INTEGRATION_ADDED = TransportVersion.fromName("voyage_ai_integration_added");

    public static VoyageAIRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var modelId = extractRequiredString(map, ServiceFields.MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var rateLimitSettings = extractRateLimitSettings(map, context, validationException);

        validationException.throwIfValidationErrorsExist();

        return new VoyageAIRerankServiceSettings(modelId, rateLimitSettings);
    }

    public VoyageAIRerankServiceSettings(String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        super(modelId, rateLimitSettings);
    }

    public VoyageAIRerankServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public VoyageAIRerankServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings(),
            validationException,
            ConfigurationParseContext.REQUEST
        );

        validationException.throwIfValidationErrorsExist();

        return new VoyageAIRerankServiceSettings(this.modelId(), extractedRateLimitSettings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return VOYAGE_AI_INTEGRATION_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(VOYAGE_AI_INTEGRATION_ADDED);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VoyageAIRerankServiceSettings that = (VoyageAIRerankServiceSettings) o;
        return Objects.equals(modelId(), that.modelId()) && Objects.equals(rateLimitSettings(), that.rateLimitSettings());
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId(), rateLimitSettings());
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
