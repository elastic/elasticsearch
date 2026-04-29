/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.embeddings;

import org.elasticsearch.TransportVersion;
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
import org.elasticsearch.xpack.inference.services.googleaistudio.GoogleAiStudioRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.googleaistudio.GoogleAiStudioService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;

public class GoogleAiStudioEmbeddingsServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        GoogleAiStudioRateLimitServiceSettings {

    public static final String NAME = "google_ai_studio_embeddings_service_settings";

    /**
     * Rate limits are defined at <a href="https://ai.google.dev/pricing">Google Gemini API Pricing</a>.
     * For pay-as-you-go you've 360 requests per minute.
     */
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(360);

    public static GoogleAiStudioEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var similarityMeasure = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            GoogleAiStudioService.NAME,
            context
        );

        validationException.throwIfValidationErrorsExist();

        return new GoogleAiStudioEmbeddingsServiceSettings(modelId, maxInputTokens, dimensions, similarityMeasure, rateLimitSettings);
    }

    private final String modelId;

    private final RateLimitSettings rateLimitSettings;

    private final Integer dimensions;

    private final Integer maxInputTokens;

    private final SimilarityMeasure similarity;

    public GoogleAiStudioEmbeddingsServiceSettings(
        String modelId,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dimensions,
        @Nullable SimilarityMeasure similarity,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.modelId = modelId;
        this.maxInputTokens = maxInputTokens;
        this.dimensions = dimensions;
        this.similarity = similarity;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public GoogleAiStudioEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.maxInputTokens = in.readOptionalVInt();
        this.dimensions = in.readOptionalVInt();
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public ServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var extractedMaxInputTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            GoogleAiStudioService.NAME,
            ConfigurationParseContext.REQUEST
        );

        validationException.throwIfValidationErrorsExist();

        return new GoogleAiStudioEmbeddingsServiceSettings(
            this.modelId,
            extractedMaxInputTokens != null ? extractedMaxInputTokens : this.maxInputTokens,
            this.dimensions,
            this.similarity,
            extractedRateLimitSettings
        );
    }

    @Override
    public String modelId() {
        return modelId;
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
        return dimensions;
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

        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalEnum(similarity);
        rateLimitSettings.writeTo(out);
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID, modelId);

        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }

        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
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
        GoogleAiStudioEmbeddingsServiceSettings that = (GoogleAiStudioEmbeddingsServiceSettings) object;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && similarity == that.similarity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, rateLimitSettings, dimensions, maxInputTokens, similarity);
    }
}
