/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.parser.EnumParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

public class AmazonBedrockEmbeddingsServiceSettings extends AmazonBedrockServiceSettings {
    public static final String NAME = "amazon_bedrock_embeddings_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * Creates an {@link ObjectParser} for the Llama embeddings service settings.
     *
     * @param ignoreUnknownFields whether the parser should tolerate unknown fields. This is {@code false} for request parsing (so that
     *                            unexpected fields are rejected) and {@code true} for persisted configuration (so that fields written by
     *                            other versions are tolerated).
     * @return the parser
     */
    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            ignoreUnknownFields,
            Builder::new
        );
        AmazonBedrockServiceSettings.declareCommonFields(parser);
        parser.declareInt(Builder::setDimensions, new ParseField(DIMENSIONS));
        parser.declareString(Builder::setSimilarity, EnumParser::parseSimilarity, new ParseField(SIMILARITY));
        parser.declareInt(Builder::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
        return parser;
    }

    private final Integer dimensions;
    private final Boolean dimensionsSetByUser;
    private final Integer maxInputTokens;
    private final SimilarityMeasure similarity;

    public static AmazonBedrockEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return AmazonBedrockServiceSettings.fromMap(map, context, parser);
    }

    public AmazonBedrockEmbeddingsServiceSettings(StreamInput in) throws IOException {
        super(in);
        dimensions = in.readOptionalVInt();
        dimensionsSetByUser = in.readBoolean();
        maxInputTokens = in.readOptionalVInt();
        similarity = in.readOptionalEnum(SimilarityMeasure.class);
    }

    public AmazonBedrockEmbeddingsServiceSettings(
        String region,
        String model,
        AmazonBedrockProvider provider,
        @Nullable Integer dimensions,
        Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity,
        RateLimitSettings rateLimitSettings
    ) {
        super(region, model, provider, rateLimitSettings);
        this.dimensions = dimensions;
        this.dimensionsSetByUser = dimensionsSetByUser;
        this.maxInputTokens = maxInputTokens;
        this.similarity = similarity;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalVInt(dimensions);
        out.writeBoolean(dimensionsSetByUser);
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalEnum(similarity);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        super.addBaseXContent(builder, params);
        builder.field(ServiceFields.DIMENSIONS_SET_BY_USER, dimensionsSetByUser);

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        super.addXContentFragmentOfExposedFields(builder, params);

        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }

        return builder;
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    @Override
    public Boolean dimensionsSetByUser() {
        return this.dimensionsSetByUser;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    @Override
    public AmazonBedrockEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();
        var updatedCommonSettings = updateCommonSettings(serviceSettings, validationException);

        var extractedMaxTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        validationException.throwIfValidationErrorsExist();
        return new AmazonBedrockEmbeddingsServiceSettings(
            updatedCommonSettings.region(),
            updatedCommonSettings.model(),
            updatedCommonSettings.provider(),
            this.dimensions,
            this.dimensionsSetByUser,
            extractedMaxTokens != null ? extractedMaxTokens : this.maxInputTokens,
            this.similarity,
            updatedCommonSettings.rateLimitSettings()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AmazonBedrockEmbeddingsServiceSettings that = (AmazonBedrockEmbeddingsServiceSettings) o;

        return Objects.equals(region, that.region)
            && Objects.equals(provider, that.provider)
            && Objects.equals(model, that.model)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimensions, dimensionsSetByUser, maxInputTokens, similarity);
    }

    /**
     * Accumulates the embeddings-specific fields on top of the common Llama fields and builds a
     * {@link AmazonBedrockEmbeddingsServiceSettings}, enforcing that {@code dimensions} and {@code max_input_tokens} are positive.
     */
    public static class Builder extends AmazonBedrockServiceSettings.Builder<AmazonBedrockEmbeddingsServiceSettings> {
        private Integer dimensions;
        private SimilarityMeasure similarity;
        private Integer maxInputTokens;

        public void setDimensions(Integer dimensions) {
            validatePositiveInteger(dimensions, DIMENSIONS);
            this.dimensions = dimensions;
        }

        public void setSimilarity(SimilarityMeasure similarity) {
            this.similarity = similarity;
        }

        public void setMaxInputTokens(Integer maxInputTokens) {
            validatePositiveInteger(maxInputTokens, MAX_INPUT_TOKENS);
            this.maxInputTokens = maxInputTokens;
        }

        @Override
        protected AmazonBedrockEmbeddingsServiceSettings build(
            String region,
            String model,
            String provider,
            RateLimitSettings rateLimitSettings
        ) {
            return new AmazonBedrockEmbeddingsServiceSettings(
                region,
                model,
                AmazonBedrockProvider.fromString(provider),
                dimensions,
                false,
                maxInputTokens,
                similarity,
                rateLimitSettings
            );
        }
    }

}
