/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.embeddings;

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
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

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
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiService.INFERENCE_API_FIREWORKS_AI_SERVICE_ADDED;

/**
 * Defines the service settings for interacting with FireworksAI's embeddings API.
 */
public class FireworksAiEmbeddingsServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        FireworksAiRateLimitServiceSettings {

    public static final String NAME = "fireworksai_embeddings_service_settings";

    public static final String DIMENSIONS_SET_BY_USER = "dimensions_set_by_user";
    private static final String DEFAULT_URL = "https://api.fireworks.ai/inference/v1/embeddings";

    // FireworksAI default rate limit is 6000 RPM per their documentation
    // See: https://docs.fireworks.ai/guides/quotas_usage/rate-limits
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(6000);

    public static FireworksAiEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        // Extract common fields
        String url = extractOptionalString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        Integer dims = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            FireworksAiService.NAME,
            context
        );

        // Optimize URI conversion - only convert if URL is provided, otherwise use default
        URI uri = url != null
            ? convertToUri(url, URL, ModelConfigurations.SERVICE_SETTINGS, validationException)
            : ServiceUtils.createUri(DEFAULT_URL);

        // Handle dimensionsSetByUser based on context
        boolean dimensionsSetByUser = switch (context) {
            case PERSISTENT -> {
                Boolean value = extractOptionalBoolean(map, DIMENSIONS_SET_BY_USER, validationException);
                yield value != null ? value : false;
            }
            case REQUEST -> dims != null;
        };

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new FireworksAiEmbeddingsServiceSettings(
            modelId,
            uri,
            similarity,
            dims,
            maxInputTokens,
            dimensionsSetByUser,
            rateLimitSettings
        );
    }

    private final String modelId;
    private final URI uri;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final Boolean dimensionsSetByUser;
    private final RateLimitSettings rateLimitSettings;

    public FireworksAiEmbeddingsServiceSettings(
        String modelId,
        URI uri,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        Boolean dimensionsSetByUser,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.modelId = Objects.requireNonNull(modelId);
        this.uri = Objects.requireNonNull(uri);
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.dimensionsSetByUser = dimensionsSetByUser;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public FireworksAiEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.uri = ServiceUtils.createUri(in.readString());
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.dimensions = in.readOptionalVInt();
        this.maxInputTokens = in.readOptionalVInt();
        this.dimensionsSetByUser = in.readBoolean();
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    public String modelId() {
        return modelId;
    }

    @Override
    public URI uri() {
        return uri;
    }

    public SimilarityMeasure similarity() {
        return similarity;
    }

    public Integer dimensions() {
        return dimensions;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public Boolean dimensionsSetByUser() {
        return dimensionsSetByUser;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
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

        builder.field(DIMENSIONS_SET_BY_USER, dimensionsSetByUser);

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID, modelId);
        builder.field(URL, uri.toString());

        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }

        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }

        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }

        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeString(uri.toString());
        out.writeOptionalEnum(similarity);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
        out.writeBoolean(dimensionsSetByUser);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        FireworksAiEmbeddingsServiceSettings that = (FireworksAiEmbeddingsServiceSettings) object;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && dimensionsSetByUser == that.dimensionsSetByUser
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, similarity, dimensions, maxInputTokens, dimensionsSetByUser, rateLimitSettings);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return INFERENCE_API_FIREWORKS_AI_SERVICE_ADDED;
    }
}
