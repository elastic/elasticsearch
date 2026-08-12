/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

/**
 * Settings for the TencentCloud embeddings service. Extends {@link TencentCloudCommonServiceSettings} and adds the
 * embeddings-specific fields: dimensions, similarity measure, and max input tokens.
 */
public class TencentCloudEmbeddingsServiceSettings extends TencentCloudCommonServiceSettings {

    public static final String NAME = "tencentcloud_embeddings_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            ignoreUnknownFields,
            Builder::new
        );
        TencentCloudCommonServiceSettings.declareCommonFields(parser, TencentCloudCommonServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS);
        parser.declareString(Builder::setSimilarity, SimilarityMeasure::fromString, new ParseField(SIMILARITY));
        parser.declareInt(Builder::setDimensions, new ParseField(DIMENSIONS));
        parser.declareInt(Builder::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
        return parser;
    }

    public static TencentCloudEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        var validationException = new ValidationException();
        var settings = TencentCloudCommonServiceSettings.fromMap(map, context, parser, validationException);
        validationException.throwIfValidationErrorsExist();
        return settings;
    }

    @Nullable
    private final SimilarityMeasure similarity;
    @Nullable
    private final Integer dimensions;
    @Nullable
    private final Integer maxInputTokens;

    public TencentCloudEmbeddingsServiceSettings(
        String modelId,
        String region,
        @Nullable RateLimitSettings rateLimitSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens
    ) {
        super(modelId, region, rateLimitSettings);
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
    }

    public TencentCloudEmbeddingsServiceSettings(StreamInput in) throws IOException {
        super(in);
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.dimensions = in.readOptionalVInt();
        this.maxInputTokens = in.readOptionalVInt();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    @Nullable
    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public TencentCloudEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();

        var extractedMaxInputTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings(),
            validationException,
            ConfigurationParseContext.REQUEST
        );

        validationException.throwIfValidationErrorsExist();

        return new TencentCloudEmbeddingsServiceSettings(
            this.modelId(),
            this.region(),
            extractedRateLimitSettings,
            this.similarity(),
            this.dimensions(),
            extractedMaxInputTokens != null ? extractedMaxInputTokens : this.maxInputTokens()
        );
    }

    /**
     * Return an updated copy with the resolved embedding size and similarity measure.
     */
    public TencentCloudEmbeddingsServiceSettings updateEmbeddingDetails(int newDimensions, SimilarityMeasure newSimilarity) {
        return new TencentCloudEmbeddingsServiceSettings(
            this.modelId(),
            this.region(),
            this.rateLimitSettings(),
            newSimilarity,
            newDimensions,
            this.maxInputTokens()
        );
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        super.toXContentFragmentOfExposedFields(builder, params);
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalEnum(similarity);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;
        TencentCloudEmbeddingsServiceSettings that = (TencentCloudEmbeddingsServiceSettings) o;
        return similarity == that.similarity
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), similarity, dimensions, maxInputTokens);
    }

    public static class Builder extends TencentCloudCommonServiceSettings.Builder<TencentCloudEmbeddingsServiceSettings> {
        private SimilarityMeasure similarity;
        private Integer dimensions;
        private Integer maxInputTokens;

        public void setSimilarity(SimilarityMeasure similarity) {
            this.similarity = similarity;
        }

        public void setDimensions(Integer dimensions) {
            this.dimensions = dimensions;
        }

        public void setMaxInputTokens(Integer maxInputTokens) {
            this.maxInputTokens = maxInputTokens;
        }

        @Override
        protected TencentCloudEmbeddingsServiceSettings build() {
            return new TencentCloudEmbeddingsServiceSettings(modelId, region, rateLimitSettings, similarity, dimensions, maxInputTokens);
        }
    }
}
