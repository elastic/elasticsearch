/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.API_VERSION;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.DEPLOYMENT_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.RESOURCE_NAME;

public class AzureOpenAiCompletionServiceSettings implements ServiceSettings, AzureOpenAiRateLimitServiceSettings {

    public static final String NAME = "azure_openai_completions_service_settings";

    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1_440);

    public static AzureOpenAiCompletionServiceSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        var settings = fromMap(map, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureOpenAiCompletionServiceSettings(settings);
    }

    private static AzureOpenAiCompletionServiceSettings.CommonFields fromMap(
        Map<String, Object> map,
        ValidationException validationException
    ) {
        String resourceName = extractRequiredString(map, RESOURCE_NAME, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String deploymentId = extractRequiredString(map, DEPLOYMENT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String apiVersion = extractRequiredString(map, API_VERSION, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException);

        return new AzureOpenAiCompletionServiceSettings.CommonFields(resourceName, deploymentId, apiVersion, rateLimitSettings);
    }

    private record CommonFields(String resourceName, String deploymentId, String apiVersion, RateLimitSettings rateLimitSettings) {}

    private final String resourceName;
    private final String deploymentId;
    private final String apiVersion;

    private final RateLimitSettings rateLimitSettings;

    public AzureOpenAiCompletionServiceSettings(String resourceName, String deploymentId, String apiVersion, @Nullable RateLimitSettings rateLimitSettings) {
        this.resourceName = resourceName;
        this.deploymentId = deploymentId;
        this.apiVersion = apiVersion;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public AzureOpenAiCompletionServiceSettings(StreamInput in) throws IOException {
        resourceName = in.readString();
        deploymentId = in.readString();
        apiVersion = in.readString();

        if (in.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_RATE_LIMIT_SETTINGS_ADDED)) {
            rateLimitSettings = new RateLimitSettings(in);
        } else {
            rateLimitSettings = DEFAULT_RATE_LIMIT_SETTINGS;
        }
    }

    private AzureOpenAiCompletionServiceSettings(AzureOpenAiCompletionServiceSettings.CommonFields fields) {
        this(fields.resourceName, fields.deploymentId, fields.apiVersion, fields.rateLimitSettings);
    }

    public String resourceName() {
        return resourceName;
    }

    public String deploymentId() {
        return deploymentId;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return DEFAULT_RATE_LIMIT_SETTINGS;
    }

    public String apiVersion() {
        return apiVersion;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();

        toXContentFragmentOfExposedFields(builder, params);

        builder.endObject();
        return builder;
    }

    private void toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(RESOURCE_NAME, resourceName);
        builder.field(DEPLOYMENT_ID, deploymentId);
        builder.field(API_VERSION, apiVersion);
        rateLimitSettings.toXContent(builder, params);
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return (builder, params) -> {
            builder.startObject();

            toXContentFragmentOfExposedFields(builder, params);

            builder.endObject();
            return builder;
        };
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_AZURE_OPENAI_EMBEDDINGS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(resourceName);
        out.writeString(deploymentId);
        out.writeString(apiVersion);

        if (out.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_RATE_LIMIT_SETTINGS_ADDED)) {
            rateLimitSettings.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        AzureOpenAiCompletionServiceSettings that = (AzureOpenAiCompletionServiceSettings) object;
        return Objects.equals(resourceName, that.resourceName) && Objects.equals(deploymentId, that.deploymentId) && Objects.equals(apiVersion, that.apiVersion) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceName, deploymentId, apiVersion, rateLimitSettings);
    }
}
