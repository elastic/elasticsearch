/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.SettingsScope.SERVICE_SETTINGS;
import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.INFERENCE_API_MISTRAL_URL_ADDED;
import static org.elasticsearch.xpack.inference.services.mistral.MistralConstants.MODEL_FIELD;

public class MistralEmbeddingsServiceSettings extends FilteredXContentObject implements ServiceSettings {
    public static final String NAME = "mistral_embeddings_service_settings";

    private final String modelId;
    @Nullable
    private final URI url;
    private final Integer dimensions;
    private final SimilarityMeasure similarity;
    private final Integer maxInputTokens;
    private final RateLimitSettings rateLimitSettings;

    // default for Mistral is 5 requests / sec
    // setting this to 240 (4 requests / sec) is a sane default for us
    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(240);

    public static MistralEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var modelId = extractRequiredString(map, MODEL_FIELD, SERVICE_SETTINGS, validationException);
        var url = extractOptionalUri(map, URL, validationException);
        var similarity = extractSimilarity(map, SERVICE_SETTINGS, validationException);
        var maxInputTokens = extractOptionalPositiveInteger(map, MAX_INPUT_TOKENS, SERVICE_SETTINGS, validationException);
        var rateLimitSettings = RateLimitSettings.of(map, DEFAULT_RATE_LIMIT_SETTINGS, validationException, context);
        var dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, SERVICE_SETTINGS, validationException);

        validationException.throwIfValidationErrorsExist();

        return new MistralEmbeddingsServiceSettings(modelId, url, dimensions, maxInputTokens, similarity, rateLimitSettings);
    }

    @Override
    public MistralEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var extractedMaxInputTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            SERVICE_SETTINGS,
            validationException
        );
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            ConfigurationParseContext.REQUEST
        );

        validationException.throwIfValidationErrorsExist();

        return new MistralEmbeddingsServiceSettings(
            this.modelId,
            this.url,
            this.dimensions,
            extractedMaxInputTokens != null ? extractedMaxInputTokens : this.maxInputTokens,
            this.similarity,
            extractedRateLimitSettings
        );
    }

    public MistralEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.dimensions = in.readOptionalVInt();
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.maxInputTokens = in.readOptionalVInt();
        this.rateLimitSettings = new RateLimitSettings(in);
        this.url = in.getTransportVersion().supports(INFERENCE_API_MISTRAL_URL_ADDED) ? createOptionalUri(in.readOptionalString()) : null;
    }

    public MistralEmbeddingsServiceSettings(
        String modelId,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this(modelId, null, dimensions, maxInputTokens, similarity, rateLimitSettings);
    }

    public MistralEmbeddingsServiceSettings(
        String modelId,
        @Nullable URI url,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.modelId = modelId;
        this.url = url;
        this.dimensions = dimensions;
        this.similarity = similarity;
        this.maxInputTokens = maxInputTokens;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
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
    public String modelId() {
        return this.modelId;
    }

    /**
     * The custom endpoint url, or {@code null} when the default Mistral endpoint should be used.
     */
    @Nullable
    public URI url() {
        return this.url;
    }

    @Override
    public Integer dimensions() {
        return this.dimensions;
    }

    public Integer maxInputTokens() {
        return this.maxInputTokens;
    }

    @Override
    public SimilarityMeasure similarity() {
        return this.similarity;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalEnum(similarity);
        out.writeOptionalVInt(maxInputTokens);
        rateLimitSettings.writeTo(out);
        if (out.getTransportVersion().supports(INFERENCE_API_MISTRAL_URL_ADDED)) {
            out.writeOptionalString(url != null ? url.toString() : null);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        this.toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_FIELD, this.modelId);

        if (url != null) {
            builder.field(URL, url.toString());
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (this.maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, this.maxInputTokens);
        }
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MistralEmbeddingsServiceSettings that = (MistralEmbeddingsServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(url, that.url)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, url, dimensions, maxInputTokens, similarity, rateLimitSettings);
    }

}
