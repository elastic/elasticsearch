/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.embeddings;

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
import org.elasticsearch.xpack.inference.common.parser.EnumParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.llama.LlamaServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;

/**
 * Settings for the Llama embeddings service. Extends {@link LlamaServiceSettings} and adds the
 * embeddings-specific fields: dimensions, similarity measure, and max input tokens.
 */
public class LlamaEmbeddingsServiceSettings extends LlamaServiceSettings {
    public static final String NAME = "llama_embeddings_service_settings";

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
        LlamaServiceSettings.declareCommonFields(parser);
        parser.declareInt(Builder::setDimensions, new ParseField(DIMENSIONS));
        parser.declareString(Builder::setSimilarity, EnumParser::parseSimilarity, new ParseField(SIMILARITY));
        parser.declareInt(Builder::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
        return parser;
    }

    private final Integer dimensions;
    private final SimilarityMeasure similarity;
    private final Integer maxInputTokens;

    /**
     * Creates a new instance of LlamaEmbeddingsServiceSettings from a map of settings.
     *
     * @param map the map containing the settings
     * @param context the context for parsing configuration settings
     * @return a new instance of LlamaEmbeddingsServiceSettings
     */
    public static LlamaEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return LlamaServiceSettings.fromMap(map, context, parser);
    }

    @Override
    public LlamaEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse Llama embeddings service settings update", e);
        }
    }

    /**
     * Constructs a new LlamaEmbeddingsServiceSettings from a StreamInput. The fields are read in the historical interleaved
     * order (model_id, url, dimensions, similarity, max_input_tokens, rate_limit); this constructor delegates to the in-memory
     * constructor relying on Java's left-to-right argument evaluation to preserve that order.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public LlamaEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this(
            in.readString(),
            createUri(in.readString()),
            in.readOptionalVInt(),
            in.readOptionalEnum(SimilarityMeasure.class),
            in.readOptionalVInt(),
            new RateLimitSettings(in)
        );
    }

    /**
     * Constructs a new LlamaEmbeddingsServiceSettings with the specified parameters.
     *
     * @param modelId the identifier for the model
     * @param uri the URI of the Llama service
     * @param dimensions the number of dimensions for the embeddings, can be null
     * @param similarity the similarity measure to use, can be null
     * @param maxInputTokens the maximum number of input tokens, can be null
     * @param rateLimitSettings the rate limit settings for the service, can be null
     */
    public LlamaEmbeddingsServiceSettings(
        String modelId,
        URI uri,
        @Nullable Integer dimensions,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer maxInputTokens,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        super(modelId, uri, rateLimitSettings);
        this.dimensions = dimensions;
        this.similarity = similarity;
        this.maxInputTokens = maxInputTokens;
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
        out.writeString(modelId());
        out.writeString(uri().toString());
        out.writeOptionalVInt(dimensions);
        out.writeOptionalEnum(similarity);
        out.writeOptionalVInt(maxInputTokens);
        rateLimitSettings().writeTo(out);
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
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;
        LlamaEmbeddingsServiceSettings that = (LlamaEmbeddingsServiceSettings) o;
        return Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(similarity, that.similarity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimensions, maxInputTokens, similarity);
    }

    /**
     * Accumulates the embeddings-specific fields on top of the common Llama fields and builds a
     * {@link LlamaEmbeddingsServiceSettings}, enforcing that {@code dimensions} and {@code max_input_tokens} are positive.
     */
    public static class Builder extends LlamaServiceSettings.Builder<LlamaEmbeddingsServiceSettings> {
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
        protected LlamaEmbeddingsServiceSettings build(String modelId, URI uri, RateLimitSettings rateLimitSettings) {
            return new LlamaEmbeddingsServiceSettings(modelId, uri, dimensions, similarity, maxInputTokens, rateLimitSettings);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code max_input_tokens} and {@code rate_limit} fields. Including any
     * immutable field (such as {@code model_id}, {@code url}, {@code dimensions}, or {@code similarity}) causes the strict parser to
     * reject the request.
     */
    private static class Update extends LlamaServiceSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            LlamaServiceSettings.declareCommonUpdatableFields(PARSER);
            PARSER.declareInt(Update::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
        }

        private Integer maxInputTokens;

        private void setMaxInputTokens(Integer maxInputTokens) {
            validatePositiveInteger(maxInputTokens, MAX_INPUT_TOKENS);
            this.maxInputTokens = maxInputTokens;
        }

        public LlamaEmbeddingsServiceSettings mergeInto(LlamaEmbeddingsServiceSettings existing) {
            var updatedMaxInputTokens = this.maxInputTokens != null ? this.maxInputTokens : existing.maxInputTokens();
            return new LlamaEmbeddingsServiceSettings(
                existing.modelId(),
                existing.uri(),
                existing.dimensions(),
                existing.similarity(),
                updatedMaxInputTokens,
                mergedRateLimitSettings(existing)
            );
        }
    }
}
