/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

/**
 * Defines the service settings for interacting with OpenAI's text embedding models.
 */
public class OpenAiEmbeddingsServiceSettings implements ServiceSettings {

    public static final String NAME = "openai_service_settings";

    static final String ORGANIZATION = "organization_id";
    static final String DIMENSIONS_SET_BY_USER = "dimensions_set_by_user";

    public static OpenAiEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return switch (context) {
            case REQUEST -> fromRequestMap(map);
            case PERSISTENT -> fromPersistentMap(map);
        };
    }

    private static OpenAiEmbeddingsServiceSettings fromPersistentMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        var commonFields = fromMap(map, validationException);

        Boolean dimensionsSetByUser = removeAsType(map, DIMENSIONS_SET_BY_USER, Boolean.class);
        if (dimensionsSetByUser == null) {
            validationException.addValidationError(
                ServiceUtils.missingSettingErrorMsg(DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
            );
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiEmbeddingsServiceSettings(commonFields, Boolean.TRUE.equals(dimensionsSetByUser));
    }

    private static OpenAiEmbeddingsServiceSettings fromRequestMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        var commonFields = fromMap(map, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiEmbeddingsServiceSettings(commonFields, commonFields.dimensions != null);
    }

    private static CommonFields fromMap(Map<String, Object> map, ValidationException validationException) {

        String url = extractOptionalString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String organizationId = extractOptionalString(map, ORGANIZATION, ModelConfigurations.SERVICE_SETTINGS, validationException);
        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);
        Integer dims = removeAsType(map, DIMENSIONS, Integer.class);
        URI uri = convertToUri(url, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        return new CommonFields(modelId, uri, organizationId, similarity, maxInputTokens, dims);
    }

    private record CommonFields(
        String modelId,
        @Nullable URI uri,
        @Nullable String organizationId,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dimensions
    ) {}

    private final String modelId;
    private final URI uri;
    private final String organizationId;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final Boolean dimensionsSetByUser;

    public OpenAiEmbeddingsServiceSettings(
        String modelId,
        @Nullable URI uri,
        @Nullable String organizationId,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        Boolean dimensionsSetByUser
    ) {
        this.uri = uri;
        this.modelId = modelId;
        this.organizationId = organizationId;
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.dimensionsSetByUser = Objects.requireNonNull(dimensionsSetByUser);
    }

    OpenAiEmbeddingsServiceSettings(
        String modelId,
        @Nullable String uri,
        @Nullable String organizationId,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        Boolean dimensionsSetByUser
    ) {
        this(modelId, createOptionalUri(uri), organizationId, similarity, dimensions, maxInputTokens, dimensionsSetByUser);
    }

    public OpenAiEmbeddingsServiceSettings(StreamInput in) throws IOException {
        uri = createOptionalUri(in.readOptionalString());
        organizationId = in.readOptionalString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            similarity = in.readOptionalEnum(SimilarityMeasure.class);
            dimensions = in.readOptionalVInt();
            maxInputTokens = in.readOptionalVInt();
        } else {
            similarity = null;
            dimensions = null;
            maxInputTokens = null;
        }

        if (in.getTransportVersion().onOrAfter(TransportVersions.ML_DIMENSIONS_SET_BY_USER_ADDED)) {
            dimensionsSetByUser = in.readBoolean();
        } else {
            dimensionsSetByUser = false;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.ML_MODEL_IN_SERVICE_SETTINGS)) {
            modelId = in.readString();
        } else {
            modelId = "unset";
        }
    }

    private OpenAiEmbeddingsServiceSettings(CommonFields fields, Boolean dimensionsSetByUser) {
        this(
            fields.modelId,
            fields.uri,
            fields.organizationId,
            fields.similarity,
            fields.dimensions,
            fields.maxInputTokens,
            dimensionsSetByUser
        );
    }

    public URI uri() {
        return uri;
    }

    public String organizationId() {
        return organizationId;
    }

    public SimilarityMeasure similarity() {
        return similarity;
    }

    public Integer dimensions() {
        return dimensions;
    }

    public Boolean dimensionsSetByUser() {
        return dimensionsSetByUser;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

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

        toXContentFragmentOfExposedFields(builder, params);

        if (dimensionsSetByUser != null) {
            builder.field(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }

        builder.endObject();
        return builder;
    }

    private void toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID, modelId);
        if (uri != null) {
            builder.field(URL, uri.toString());
        }
        if (organizationId != null) {
            builder.field(ORGANIZATION, organizationId);
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
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
        return TransportVersions.V_8_12_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        var uriToWrite = uri != null ? uri.toString() : null;
        out.writeOptionalString(uriToWrite);
        out.writeOptionalString(organizationId);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeOptionalEnum(similarity);
            out.writeOptionalVInt(dimensions);
            out.writeOptionalVInt(maxInputTokens);
        }

        if (out.getTransportVersion().onOrAfter(TransportVersions.ML_DIMENSIONS_SET_BY_USER_ADDED)) {
            out.writeBoolean(dimensionsSetByUser);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.ML_MODEL_IN_SERVICE_SETTINGS)) {
            out.writeString(modelId);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenAiEmbeddingsServiceSettings that = (OpenAiEmbeddingsServiceSettings) o;
        return Objects.equals(uri, that.uri)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(organizationId, that.organizationId)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, modelId, organizationId, similarity, dimensions, maxInputTokens, dimensionsSetByUser);
    }
}
