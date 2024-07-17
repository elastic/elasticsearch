/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.embeddings;

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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.LOCATION;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.PROJECT_ID;

public class GoogleVertexAiEmbeddingsServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        GoogleVertexAiEmbeddingsRateLimitServiceSettings {

    public static final String NAME = "google_vertex_ai_embeddings_service_settings";

    public static final String DIMENSIONS_SET_BY_USER = "dimensions_set_by_user";

    // See online prediction requests per minute: https://cloud.google.com/vertex-ai/docs/quotas.
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(30_000);

    public static GoogleVertexAiEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String location = extractRequiredString(map, LOCATION, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String projectId = extractRequiredString(map, PROJECT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String model = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        SimilarityMeasure similarityMeasure = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dims = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            GoogleVertexAiService.NAME,
            context
        );

        Boolean dimensionsSetByUser = extractOptionalBoolean(map, DIMENSIONS_SET_BY_USER, validationException);

        switch (context) {
            case REQUEST -> {
                if (dimensionsSetByUser != null) {
                    validationException.addValidationError(
                        ServiceUtils.invalidSettingError(DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
                dimensionsSetByUser = dims != null;
            }
            case PERSISTENT -> {
                if (dimensionsSetByUser == null) {
                    validationException.addValidationError(
                        ServiceUtils.missingSettingErrorMsg(DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
            }
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new GoogleVertexAiEmbeddingsServiceSettings(
            location,
            projectId,
            model,
            dimensionsSetByUser,
            maxInputTokens,
            dims,
            similarityMeasure,
            rateLimitSettings
        );
    }

    private final String location;

    private final String projectId;

    private final String modelId;

    private final Integer dims;

    private final SimilarityMeasure similarity;
    private final Integer maxInputTokens;

    private final RateLimitSettings rateLimitSettings;

    private final Boolean dimensionsSetByUser;

    public GoogleVertexAiEmbeddingsServiceSettings(
        String location,
        String projectId,
        String modelId,
        Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dims,
        @Nullable SimilarityMeasure similarity,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.location = location;
        this.projectId = projectId;
        this.modelId = modelId;
        this.dimensionsSetByUser = dimensionsSetByUser;
        this.maxInputTokens = maxInputTokens;
        this.dims = dims;
        this.similarity = Objects.requireNonNullElse(similarity, SimilarityMeasure.DOT_PRODUCT);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public GoogleVertexAiEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.location = in.readString();
        this.projectId = in.readString();
        this.modelId = in.readString();
        this.dimensionsSetByUser = in.readBoolean();
        this.maxInputTokens = in.readOptionalVInt();
        this.dims = in.readOptionalVInt();
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public String projectId() {
        return projectId;
    }

    public String location() {
        return location;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    public Boolean dimensionsSetByUser() {
        return dimensionsSetByUser;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public Integer dimensions() {
        return dims;
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragmentOfExposedFields(builder, params);
        builder.field(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);

        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_GOOGLE_VERTEX_AI_EMBEDDINGS_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(location);
        out.writeString(projectId);
        out.writeString(modelId);
        out.writeBoolean(dimensionsSetByUser);
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalVInt(dims);
        out.writeOptionalEnum(similarity);
        rateLimitSettings.writeTo(out);
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(LOCATION, location);
        builder.field(PROJECT_ID, projectId);
        builder.field(MODEL_ID, modelId);

        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }

        if (dims != null) {
            builder.field(DIMENSIONS, dims);
        }

        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }

        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        GoogleVertexAiEmbeddingsServiceSettings that = (GoogleVertexAiEmbeddingsServiceSettings) object;
        return Objects.equals(location, that.location)
            && Objects.equals(projectId, that.projectId)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(dims, that.dims)
            && similarity == that.similarity
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location, projectId, modelId, dims, similarity, maxInputTokens, rateLimitSettings, dimensionsSetByUser);
    }
}
