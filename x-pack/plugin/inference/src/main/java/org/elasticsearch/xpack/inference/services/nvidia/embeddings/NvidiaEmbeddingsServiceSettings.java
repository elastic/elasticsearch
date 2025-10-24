/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaService;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;

/**
 * Settings for the Nvidia embeddings service.
 * This class encapsulates the configuration settings required to use Nvidia for generating embeddings.
 */
public class NvidiaEmbeddingsServiceSettings extends NvidiaServiceSettings {
    public static final String NAME = "nvidia_embeddings_service_settings";

    private final SimilarityMeasure similarity;
    private final Integer maxInputTokens;

    /**
     * Creates a new instance of NvidiaEmbeddingsServiceSettings from a map of settings.
     *
     * @param map the map containing the settings
     * @param context the context for parsing configuration settings
     * @return a new instance of NvidiaEmbeddingsServiceSettings
     * @throws ValidationException if any required fields are missing or invalid
     */
    public static NvidiaEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        var model = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var uri = extractOptionalUri(map, URL, validationException);
        var similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, NvidiaService.NAME, context);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new NvidiaEmbeddingsServiceSettings(model, uri, similarity, maxInputTokens, rateLimitSettings);
    }

    /**
     * Constructs a new NvidiaEmbeddingsServiceSettings from a StreamInput.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public NvidiaEmbeddingsServiceSettings(StreamInput in) throws IOException {
        super(in);
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.maxInputTokens = in.readOptionalVInt();
    }

    /**
     * Constructs a new NvidiaEmbeddingsServiceSettings with the specified parameters.
     *
     * @param modelId the identifier for the model
     * @param uri the URI of the Nvidia service
     * @param similarity the similarity measure to use, can be null
     * @param maxInputTokens the maximum number of input tokens, can be null
     * @param rateLimitSettings the rate limit settings for the service, can be null
     */
    public NvidiaEmbeddingsServiceSettings(
        String modelId,
        @Nullable URI uri,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer maxInputTokens,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        super(modelId, uri, rateLimitSettings);
        this.similarity = similarity;
        this.maxInputTokens = maxInputTokens;
    }

    /**
     * Constructs a new NvidiaEmbeddingsServiceSettings with the specified parameters.
     *
     * @param modelId the identifier for the model
     * @param url the URL of the Nvidia service
     * @param similarity the similarity measure to use, can be null
     * @param maxInputTokens the maximum number of input tokens, can be null
     * @param rateLimitSettings the rate limit settings for the service, can be null
     */
    public NvidiaEmbeddingsServiceSettings(
        String modelId,
        @Nullable String url,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer maxInputTokens,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this(modelId, createOptionalUri(url), similarity, maxInputTokens, rateLimitSettings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
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

    /**
     * Returns the rate limit settings for this service.
     *
     * @return the rate limit settings, never null
     */
    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalEnum(SimilarityMeasure.translateSimilarity(similarity, out.getTransportVersion()));
        out.writeOptionalVInt(maxInputTokens);
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        super.toXContentFragmentOfExposedFields(builder, params);
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NvidiaEmbeddingsServiceSettings that = (NvidiaEmbeddingsServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, maxInputTokens, similarity, rateLimitSettings);
    }

}
