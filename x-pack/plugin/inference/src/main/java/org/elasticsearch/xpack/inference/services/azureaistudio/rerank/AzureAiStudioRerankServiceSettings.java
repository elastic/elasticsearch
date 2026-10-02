/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class AzureAiStudioRerankServiceSettings extends AzureAiStudioServiceSettings {
    public static final String NAME = "azure_ai_studio_rerank_service_settings";
    private static final TransportVersion ML_INFERENCE_AZURE_AI_STUDIO_RERANK_ADDED = TransportVersion.fromName(
        "ml_inference_azure_ai_studio_rerank_added"
    );

    public static AzureAiStudioRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var commonSettings = AzureAiStudioServiceSettings.fromMap(map, validationException, context);

        validationException.throwIfValidationErrorsExist();

        return new AzureAiStudioRerankServiceSettings(
            new AzureAiStudioRerankServiceSettings.AzureAiStudioRerankCommonFields(commonSettings)
        );
    }

    @Override
    public AzureAiStudioRerankServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var updatedCommonSettings = updateCommonSettings(serviceSettings, validationException);

        validationException.throwIfValidationErrorsExist();

        return new AzureAiStudioRerankServiceSettings(
            new AzureAiStudioRerankServiceSettings.AzureAiStudioRerankCommonFields(updatedCommonSettings)
        );
    }

    private record AzureAiStudioRerankCommonFields(AzureAiStudioCommonSettings commonFields) {}

    public AzureAiStudioRerankServiceSettings(
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        super(target, provider, endpointType, rateLimitSettings);
    }

    public AzureAiStudioRerankServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    private AzureAiStudioRerankServiceSettings(AzureAiStudioRerankServiceSettings.AzureAiStudioRerankCommonFields fields) {
        this(
            fields.commonFields.target(),
            fields.commonFields.provider(),
            fields.commonFields.endpointType(),
            fields.commonFields.rateLimitSettings()
        );
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ML_INFERENCE_AZURE_AI_STUDIO_RERANK_ADDED;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        super.addXContentFields(builder, params);

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        super.addExposedXContentFields(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureAiStudioRerankServiceSettings that = (AzureAiStudioRerankServiceSettings) o;

        return Objects.equals(target, that.target)
            && Objects.equals(provider, that.provider)
            && Objects.equals(endpointType, that.endpointType)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(target, provider, endpointType, rateLimitSettings);
    }
}
