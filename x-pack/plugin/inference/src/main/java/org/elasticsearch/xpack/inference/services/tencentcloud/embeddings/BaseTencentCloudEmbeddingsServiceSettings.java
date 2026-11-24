/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveInteger;
import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;

/**
 * Service settings shared by the TencentCloud {@code text_embedding} and {@code embedding} tasks. The two tasks expose the same
 * fields and differ only in the endpoint they call, so subclasses only provide their writeable name.
 */
public abstract class BaseTencentCloudEmbeddingsServiceSettings extends TencentCloudCommonServiceSettings {

    /**
     * Declares the embeddings service-settings fields on the given parser.
     */
    protected static <B extends Builder<?>> void declareEmbeddingsFields(
        ObjectParser<B, ConfigurationParseContext> parser,
        boolean isPersistentParser
    ) {
        TencentCloudCommonServiceSettings.declareCommonFields(parser, TencentCloudCommonServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS);
        parser.declareString(Builder::setSimilarity, SimilarityMeasure::fromString, new ParseField(SIMILARITY));
        // The TencentCloud embeddings APIs do not accept a `dimensions` parameter, so only parse it from the persistent parser where it
        // configures the local index mapping rather than the outbound request.
        if (isPersistentParser) {
            parser.declareInt(Builder::setDimensions, new ParseField(DIMENSIONS));
        }
        parser.declareInt(Builder::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
    }

    @Nullable
    private final SimilarityMeasure similarity;
    @Nullable
    private final Integer dimensions;
    @Nullable
    private final Integer maxInputTokens;

    protected BaseTencentCloudEmbeddingsServiceSettings(
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

    protected BaseTencentCloudEmbeddingsServiceSettings(StreamInput in) throws IOException {
        super(in);
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.dimensions = in.readOptionalVInt();
        this.maxInputTokens = in.readOptionalVInt();
    }

    /**
     * Creates a settings instance of the concrete type with the given values.
     */
    protected abstract BaseTencentCloudEmbeddingsServiceSettings create(
        String modelId,
        String region,
        @Nullable RateLimitSettings rateLimitSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens
    );

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    @Nullable
    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public BaseTencentCloudEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse TencentCloud embeddings service settings update", e);
        }
    }

    public BaseTencentCloudEmbeddingsServiceSettings updateEmbeddingDetails(int newDimensions, SimilarityMeasure newSimilarity) {
        return create(modelId(), region(), rateLimitSettings(), newSimilarity, newDimensions, maxInputTokens);
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
        BaseTencentCloudEmbeddingsServiceSettings that = (BaseTencentCloudEmbeddingsServiceSettings) o;
        return similarity == that.similarity
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), similarity, dimensions, maxInputTokens);
    }

    /**
     * Accumulates the embeddings fields on behalf of a concrete settings builder.
     *
     * @param <T> the concrete settings type
     */
    public abstract static class Builder<T extends BaseTencentCloudEmbeddingsServiceSettings> extends
        TencentCloudCommonServiceSettings.Builder<T> {
        private SimilarityMeasure similarity;
        private Integer dimensions;
        private Integer maxInputTokens;

        public void setSimilarity(SimilarityMeasure similarity) {
            this.similarity = similarity;
        }

        public void setDimensions(Integer dimensions) {
            validatePositiveInteger(dimensions, DIMENSIONS);
            this.dimensions = dimensions;
        }

        public void setMaxInputTokens(Integer maxInputTokens) {
            validatePositiveInteger(maxInputTokens, MAX_INPUT_TOKENS);
            this.maxInputTokens = maxInputTokens;
        }

        protected abstract T build(
            String modelId,
            String region,
            RateLimitSettings rateLimitSettings,
            SimilarityMeasure similarity,
            Integer dimensions,
            Integer maxInputTokens
        );

        @Override
        protected final T build(String modelId, String region, RateLimitSettings rateLimitSettings) {
            return build(modelId, region, rateLimitSettings, similarity, dimensions, maxInputTokens);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code max_input_tokens} and {@code rate_limit} fields. Including any
     * immutable field (such as {@code model_id}, {@code region}, {@code dimensions}, or {@code similarity}) causes the strict parser to
     * reject the request.
     */
    private static class Update extends TencentCloudCommonServiceSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            TencentCloudCommonServiceSettings.declareCommonUpdatableFields(PARSER);
            StatefulValue.declareNullable(PARSER, (update, value) -> update.maxInputTokens = value, p -> {
                Integer value = p.intValue();
                validatePositiveInteger(value, MAX_INPUT_TOKENS);
                return value;
            }, new ParseField(MAX_INPUT_TOKENS), ObjectParser.ValueType.INT_OR_NULL);
        }

        private StatefulValue<Integer> maxInputTokens = StatefulValue.undefined();

        BaseTencentCloudEmbeddingsServiceSettings mergeInto(BaseTencentCloudEmbeddingsServiceSettings existing) {
            return existing.create(
                existing.modelId(),
                existing.region(),
                mergedRateLimitSettings(existing),
                existing.similarity(),
                existing.dimensions(),
                applyUpdate(this.maxInputTokens, existing.maxInputTokens())
            );
        }
    }
}
