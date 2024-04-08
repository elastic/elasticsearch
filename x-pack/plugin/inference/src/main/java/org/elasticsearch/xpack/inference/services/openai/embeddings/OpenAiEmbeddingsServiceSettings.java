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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

/**
 * Defines the service settings for interacting with OpenAI's text embedding models.
 */
public class OpenAiEmbeddingsServiceSettings implements ServiceSettings {

    public static final String NAME = "openai_service_settings";

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
        var commonSettings = OpenAiServiceSettings.fromMap(map);

        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);
        Integer dims = removeAsType(map, DIMENSIONS, Integer.class);

        return new CommonFields(commonSettings, similarity, maxInputTokens, dims);
    }

    private record CommonFields(
        OpenAiServiceSettings commonSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dimensions
    ) {}

    private final OpenAiServiceSettings commonSettings;

    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final Boolean dimensionsSetByUser;

    public OpenAiEmbeddingsServiceSettings(
        OpenAiServiceSettings commonSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        Boolean dimensionsSetByUser
    ) {
        this.commonSettings = commonSettings;
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.dimensionsSetByUser = Objects.requireNonNull(dimensionsSetByUser);
    }

    public OpenAiEmbeddingsServiceSettings(StreamInput in) throws IOException {
        var uri = createOptionalUri(in.readOptionalString());
        var organizationId = in.readOptionalString();
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
            commonSettings = new OpenAiServiceSettings(in.readString(), uri, organizationId);
        } else {
            commonSettings = new OpenAiServiceSettings("unset", uri, organizationId);
        }
    }

    private OpenAiEmbeddingsServiceSettings(CommonFields fields, Boolean dimensionsSetByUser) {
        this(fields.commonSettings, fields.similarity, fields.dimensions, fields.maxInputTokens, dimensionsSetByUser);
    }

    public URI uri() {
        return commonSettings.uri();
    }

    public String organizationId() {
        return commonSettings.organizationId();
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
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
        return commonSettings.modelId();
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

        builder.endObject();
        return builder;
    }

    private void toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        commonSettings.toXContentFragment(builder);

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
        var uriToWrite = uri() != null ? uri().toString() : null;
        out.writeOptionalString(uriToWrite);
        out.writeOptionalString(organizationId());

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeOptionalEnum(SimilarityMeasure.translateSimilarity(similarity, out.getTransportVersion()));
            out.writeOptionalVInt(dimensions);
            out.writeOptionalVInt(maxInputTokens);
        }

        if (out.getTransportVersion().onOrAfter(TransportVersions.ML_DIMENSIONS_SET_BY_USER_ADDED)) {
            out.writeBoolean(dimensionsSetByUser);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.ML_MODEL_IN_SERVICE_SETTINGS)) {
            out.writeString(modelId());
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        OpenAiEmbeddingsServiceSettings that = (OpenAiEmbeddingsServiceSettings) object;
        return Objects.equals(commonSettings, that.commonSettings)
            && similarity == that.similarity
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings, similarity, dimensions, maxInputTokens, dimensionsSetByUser);
    }
}
