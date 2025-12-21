/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.InferenceUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openshiftai.OpenShiftAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;

/**
 * Settings for the OpenShift AI embeddings service.
 * This class encapsulates the configuration settings required to use OpenShift AI for generating embeddings.
 */
public class OpenShiftAiEmbeddingsServiceSettings extends OpenShiftAiServiceSettings {
    public static final String NAME = "openshift_ai_embeddings_service_settings";

    private final Integer dimensions;
    private final SimilarityMeasure similarity;
    private final Integer maxInputTokens;
    private final Boolean dimensionsSetByUser;

    /**
     * Creates a new instance of OpenShiftAiEmbeddingsServiceSettings from a map of settings.
     *
     * @param map the map containing the settings
     * @param context the context for parsing configuration settings
     * @return a new instance of OpenShiftAiEmbeddingsServiceSettings
     * @throws ValidationException if any required fields are missing or invalid
     */
    public static OpenShiftAiEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();
        var commonServiceSettings = extractOpenShiftAiCommonServiceSettings(map, context, validationException);
        var dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var dimensionsSetByUser = extractOptionalBoolean(map, DIMENSIONS_SET_BY_USER, validationException);
        switch (context) {
            case REQUEST -> {
                if (dimensionsSetByUser != null) {
                    validationException.addValidationError(
                        ServiceUtils.invalidSettingError(DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
                dimensionsSetByUser = dimensions != null;
            }
            case PERSISTENT -> {
                if (dimensionsSetByUser == null) {
                    validationException.addValidationError(
                        InferenceUtils.missingSettingErrorMsg(DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
            }
        }
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenShiftAiEmbeddingsServiceSettings(
            commonServiceSettings.model(),
            commonServiceSettings.uri(),
            dimensions,
            similarity,
            maxInputTokens,
            commonServiceSettings.rateLimitSettings(),
            dimensionsSetByUser
        );
    }

    /**
     * Constructs a new OpenShiftAiEmbeddingsServiceSettings from a StreamInput.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public OpenShiftAiEmbeddingsServiceSettings(StreamInput in) throws IOException {
        super(in);
        this.dimensions = in.readOptionalVInt();
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.maxInputTokens = in.readOptionalVInt();
        this.dimensionsSetByUser = in.readBoolean();
    }

    /**
     * Constructs a new OpenShiftAiEmbeddingsServiceSettings with the specified parameters.
     *
     * @param modelId the identifier for the model
     * @param uri the URI of the OpenShift AI service
     * @param dimensions the number of dimensions for the embeddings, can be null
     * @param similarity the similarity measure to use, can be null
     * @param maxInputTokens the maximum number of input tokens, can be null
     * @param rateLimitSettings the rate limit settings for the service, can be null
     * @param dimensionsSetByUser indicates if dimensions were set by the user
     */
    public OpenShiftAiEmbeddingsServiceSettings(
        @Nullable String modelId,
        URI uri,
        @Nullable Integer dimensions,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer maxInputTokens,
        @Nullable RateLimitSettings rateLimitSettings,
        Boolean dimensionsSetByUser
    ) {
        super(modelId, uri, rateLimitSettings);
        this.dimensions = dimensions;
        this.similarity = similarity;
        this.maxInputTokens = maxInputTokens;
        this.dimensionsSetByUser = Objects.requireNonNull(dimensionsSetByUser);
    }

    /**
     * Constructs a new OpenShiftAiEmbeddingsServiceSettings with the specified parameters.
     *
     * @param modelId the identifier for the model
     * @param url the URL of the OpenShift AI service
     * @param dimensions the number of dimensions for the embeddings, can be null
     * @param similarity the similarity measure to use, can be null
     * @param maxInputTokens the maximum number of input tokens, can be null
     * @param rateLimitSettings the rate limit settings for the service, can be null
     * @param dimensionsSetByUser indicates if dimensions were set by the user
     */
    public OpenShiftAiEmbeddingsServiceSettings(
        String modelId,
        String url,
        @Nullable Integer dimensions,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer maxInputTokens,
        @Nullable RateLimitSettings rateLimitSettings,
        Boolean dimensionsSetByUser
    ) {
        this(modelId, createUri(url), dimensions, similarity, maxInputTokens, rateLimitSettings, dimensionsSetByUser);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Integer dimensions() {
        return this.dimensions;
    }

    @Override
    public Boolean dimensionsSetByUser() {
        return this.dimensionsSetByUser;
    }

    @Override
    public SimilarityMeasure similarity() {
        return this.similarity;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    /**
     * Returns the maximum number of input tokens allowed for this service.
     *
     * @return the maximum input tokens, or null if not specified
     */
    public Integer maxInputTokens() {
        return this.maxInputTokens;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalEnum(SimilarityMeasure.translateSimilarity(similarity, out.getTransportVersion()));
        out.writeOptionalVInt(maxInputTokens);
        out.writeBoolean(dimensionsSetByUser);
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        super.toXContentFragmentOfExposedFields(builder, params);

        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        builder.field(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenShiftAiEmbeddingsServiceSettings that = (OpenShiftAiEmbeddingsServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, dimensions, maxInputTokens, similarity, rateLimitSettings, dimensionsSetByUser);
    }

}
