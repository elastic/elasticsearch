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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.API_VERSION;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.DEPLOYMENT_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.RESOURCE_NAME;

public class AzureOpenAiCompletionServiceSettings implements ServiceSettings {

    public static final String NAME = "azure_openai_completions_service_settings";

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

        return new AzureOpenAiCompletionServiceSettings.CommonFields(resourceName, deploymentId, apiVersion);
    }

    private record CommonFields(String resourceName, String deploymentId, String apiVersion) {}

    private final String resourceName;
    private final String deploymentId;
    private final String apiVersion;

    public AzureOpenAiCompletionServiceSettings(String resourceName, String deploymentId, String apiVersion) {
        this.resourceName = resourceName;
        this.deploymentId = deploymentId;
        this.apiVersion = apiVersion;
    }

    public AzureOpenAiCompletionServiceSettings(StreamInput in) throws IOException {
        resourceName = in.readString();
        deploymentId = in.readString();
        apiVersion = in.readString();
    }

    private AzureOpenAiCompletionServiceSettings(AzureOpenAiCompletionServiceSettings.CommonFields fields) {
        this(fields.resourceName, fields.deploymentId, fields.apiVersion);
    }

    public String resourceName() {
        return resourceName;
    }

    public String deploymentId() {
        return deploymentId;
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
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        AzureOpenAiCompletionServiceSettings that = (AzureOpenAiCompletionServiceSettings) object;
        return Objects.equals(resourceName, that.resourceName)
            && Objects.equals(deploymentId, that.deploymentId)
            && Objects.equals(apiVersion, that.apiVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceName, deploymentId, apiVersion);
    }
}
