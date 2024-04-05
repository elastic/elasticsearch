/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.*;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.*;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.API_VERSION;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.DEPLOYMENT_ID;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.ENCODING_FORMAT;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.RESOURCE_NAME;

/**
 * Defines the service settings for interacting with OpenAI's text embedding models.
 */
public class AzureOpenAiEmbeddingsServiceSettings implements ServiceSettings {

    public static final String NAME = "azure_openai_service_settings";

    static final String DIMENSIONS_SET_BY_USER = "dimensions_set_by_user";
    static final String ENCODING_FORMAT_SET_BY_USER = "encoding_format_set_by_user";

    public static AzureOpenAiEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return switch (context) {
            case REQUEST -> fromRequestMap(map);
            case PERSISTENT -> fromPersistentMap(map);
        };
    }

    private static AzureOpenAiEmbeddingsServiceSettings fromPersistentMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        var commonFields = fromMap(map, validationException);

        Boolean dimensionsSetByUser = removeAsType(map, DIMENSIONS_SET_BY_USER, Boolean.class);
        if (dimensionsSetByUser == null && map.containsKey(DIMENSIONS)) {
            validationException.addValidationError(
                ServiceUtils.missingSettingErrorMsg(DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
            );
        }

        Boolean encodingFormatSetByUser = removeAsType(map, ENCODING_FORMAT_SET_BY_USER, Boolean.class);
        if (encodingFormatSetByUser == null && map.containsKey(ENCODING_FORMAT)) {
            validationException.addValidationError(
                ServiceUtils.missingSettingErrorMsg(ENCODING_FORMAT_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
            );
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureOpenAiEmbeddingsServiceSettings(
            commonFields,
            Boolean.TRUE.equals(dimensionsSetByUser),
            Boolean.TRUE.equals(encodingFormatSetByUser)
        );
    }

    private static AzureOpenAiEmbeddingsServiceSettings fromRequestMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        var commonFields = fromMap(map, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureOpenAiEmbeddingsServiceSettings(commonFields, commonFields.dimensions != null, commonFields.encodingFormat != null);
    }

    private static CommonFields fromMap(Map<String, Object> map, ValidationException validationException) {
        String resourceName = extractRequiredString(map, RESOURCE_NAME, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String deploymentId = extractRequiredString(map, DEPLOYMENT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String apiVersion = extractRequiredString(map, API_VERSION, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dims = removeAsType(map, DIMENSIONS, Integer.class);
        String encodingFormat = extractOptionalString(map, ENCODING_FORMAT, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer maxTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);

        return new CommonFields(resourceName, deploymentId, apiVersion, dims, encodingFormat, maxTokens);
    }

    private record CommonFields(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable Integer dimensions,
        @Nullable String encodingFormat,
        @Nullable Integer maxInputTokens
    ) {}

    private final String resourceName;
    private final String deploymentId;
    private final String apiVersion;
    private final Integer dimensions;
    private final Boolean dimensionsSetByUser;
    private final String encodingFormat;
    private final Boolean encodingFormatSetByUser;
    private final Integer maxInputTokens;

    public AzureOpenAiEmbeddingsServiceSettings(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable Integer dimensions,
        Boolean dimensionsSetByUser,
        @Nullable String encodingFormat,
        Boolean encodingFormatSetByUser,
        @Nullable Integer maxInputTokens
    ) {
        this.resourceName = resourceName;
        this.deploymentId = deploymentId;
        this.apiVersion = apiVersion;
        this.dimensions = dimensions;
        this.dimensionsSetByUser = Objects.requireNonNull(dimensionsSetByUser);
        this.encodingFormat = encodingFormat;
        this.encodingFormatSetByUser = Objects.requireNonNull(encodingFormatSetByUser);
        this.maxInputTokens = maxInputTokens;
    }

    public AzureOpenAiEmbeddingsServiceSettings(StreamInput in) throws IOException {
        resourceName = in.readString();
        deploymentId = in.readString();
        apiVersion = in.readString();
        dimensions = in.readOptionalVInt();
        dimensionsSetByUser = in.readBoolean();
        encodingFormat = in.readOptionalString();
        encodingFormatSetByUser = in.readBoolean();
        maxInputTokens = in.readOptionalVInt();
    }

    private AzureOpenAiEmbeddingsServiceSettings(CommonFields fields, Boolean dimensionsSetByUser, Boolean encodingFormatSetByUser) {
        this(
            fields.resourceName,
            fields.deploymentId,
            fields.apiVersion,
            fields.dimensions,
            dimensionsSetByUser,
            fields.encodingFormat,
            encodingFormatSetByUser,
            fields.maxInputTokens
        );
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
    public Integer dimensions() {
        return dimensions;
    }

    public Boolean dimensionsSetByUser() {
        return dimensionsSetByUser;
    }

    public String encodingFormat() {
        return encodingFormat;
    }

    public Boolean encodingFormatSetByUser() {
        return encodingFormatSetByUser;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragmentOfExposedFields(builder, params);

        if (dimensionsSetByUser != null) {
            builder.field(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }

        if (encodingFormatSetByUser != null) {
            builder.field(ENCODING_FORMAT_SET_BY_USER, encodingFormatSetByUser);
        }

        builder.endObject();
        return builder;
    }

    private void toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(RESOURCE_NAME, resourceName);
        builder.field(DEPLOYMENT_ID, deploymentId);
        builder.field(API_VERSION, apiVersion);

        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (encodingFormat != null) {
            builder.field(ENCODING_FORMAT, encodingFormat);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
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
        out.writeOptionalVInt(dimensions);
        out.writeBoolean(dimensionsSetByUser);
        out.writeOptionalString(encodingFormat);
        out.writeBoolean(encodingFormatSetByUser);
        out.writeOptionalVInt(maxInputTokens);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureOpenAiEmbeddingsServiceSettings that = (AzureOpenAiEmbeddingsServiceSettings) o;

        return Objects.equals(resourceName, that.resourceName)
            && Objects.equals(deploymentId, that.deploymentId)
            && Objects.equals(apiVersion, that.apiVersion)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser)
            && Objects.equals(encodingFormat, that.encodingFormat)
            && Objects.equals(encodingFormatSetByUser, that.encodingFormatSetByUser)
            && Objects.equals(maxInputTokens, that.maxInputTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            resourceName,
            deploymentId,
            apiVersion,
            dimensions,
            dimensionsSetByUser,
            encodingFormat,
            encodingFormatSetByUser,
            maxInputTokens
        );
    }
}
