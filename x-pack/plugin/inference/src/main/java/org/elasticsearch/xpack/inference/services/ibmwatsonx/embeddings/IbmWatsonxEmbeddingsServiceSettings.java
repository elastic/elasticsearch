/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings;

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
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveInteger;
import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;

/**
 * Settings for the IBM watsonx embeddings service. Extends {@link IbmWatsonxServiceSettings} and adds the embeddings-specific
 * fields: max input tokens, dimensions, and similarity measure.
 */
public class IbmWatsonxEmbeddingsServiceSettings extends IbmWatsonxServiceSettings {

    public static final String NAME = "ibmwatsonx_embeddings_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * Creates an {@link ObjectParser} for the IBM watsonx embeddings service settings.
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
        IbmWatsonxServiceSettings.declareCommonFields(parser);
        parser.declareInt(Builder::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
        parser.declareInt(Builder::setDimensions, new ParseField(DIMENSIONS));
        parser.declareString(Builder::setSimilarity, EnumParser::parseSimilarity, new ParseField(SIMILARITY));
        return parser;
    }

    public static IbmWatsonxEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return IbmWatsonxServiceSettings.fromMap(map, context, parser);
    }

    private final Integer maxInputTokens;
    private final Integer dimensions;
    private final SimilarityMeasure similarity;

    public IbmWatsonxEmbeddingsServiceSettings(
        String modelId,
        String projectId,
        URI uri,
        String apiVersion,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dimensions,
        @Nullable SimilarityMeasure similarity,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        super(uri, apiVersion, modelId, projectId, rateLimitSettings);
        this.maxInputTokens = maxInputTokens;
        this.dimensions = dimensions;
        this.similarity = similarity;
    }

    /**
     * Constructs a new IbmWatsonxEmbeddingsServiceSettings from a StreamInput. The fields are read in the historical order
     * (model_id, project_id, url, api_version, max_input_tokens, dimensions, similarity, rate_limit); this constructor delegates to the
     * in-memory constructor relying on Java's left-to-right argument evaluation to preserve that order.
     */
    public IbmWatsonxEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readString(),
            createUri(in.readString()),
            in.readString(),
            in.readOptionalVInt(),
            in.readOptionalVInt(),
            in.readOptionalEnum(SimilarityMeasure.class),
            new RateLimitSettings(in)
        );
    }

    @Override
    public IbmWatsonxEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse IBM watsonx embeddings service settings update", e);
        }
    }

    /**
     * Alias for {@link #uri()} preserved so existing callers using the {@code url} terminology continue to compile.
     */
    public URI url() {
        return uri();
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
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId());
        out.writeString(projectId());
        out.writeString(uri().toString());
        out.writeString(apiVersion());
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalEnum(similarity);
        rateLimitSettings().writeTo(out);
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        super.toXContentFragmentOfExposedFields(builder, params);
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;
        IbmWatsonxEmbeddingsServiceSettings that = (IbmWatsonxEmbeddingsServiceSettings) o;
        return Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(similarity, that.similarity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxInputTokens, dimensions, similarity);
    }

    /**
     * Accumulates the embeddings-specific fields on top of the common IBM watsonx fields and builds an
     * {@link IbmWatsonxEmbeddingsServiceSettings}, enforcing that {@code max_input_tokens} and {@code dimensions} are positive.
     */
    public static class Builder extends IbmWatsonxServiceSettings.Builder<IbmWatsonxEmbeddingsServiceSettings> {
        private Integer maxInputTokens;
        private Integer dimensions;
        private SimilarityMeasure similarity;

        public void setMaxInputTokens(Integer maxInputTokens) {
            validatePositiveInteger(maxInputTokens, MAX_INPUT_TOKENS);
            this.maxInputTokens = maxInputTokens;
        }

        public void setDimensions(Integer dimensions) {
            validatePositiveInteger(dimensions, DIMENSIONS);
            this.dimensions = dimensions;
        }

        public void setSimilarity(SimilarityMeasure similarity) {
            this.similarity = similarity;
        }

        @Override
        protected IbmWatsonxEmbeddingsServiceSettings build(
            URI uri,
            String apiVersion,
            String modelId,
            String projectId,
            RateLimitSettings rateLimitSettings
        ) {
            return new IbmWatsonxEmbeddingsServiceSettings(
                modelId,
                projectId,
                uri,
                apiVersion,
                maxInputTokens,
                dimensions,
                similarity,
                rateLimitSettings
            );
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code max_input_tokens} and {@code rate_limit} fields. Including any
     * immutable field (such as {@code url}, {@code api_version}, {@code model_id}, {@code project_id}, {@code dimensions}, or
     * {@code similarity}) causes the strict parser to reject the request.
     */
    private static class Update extends IbmWatsonxServiceSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            IbmWatsonxServiceSettings.declareCommonUpdatableFields(PARSER);
            StatefulValue.declareNullable(PARSER, (update, value) -> update.maxInputTokens = value, p -> {
                Integer value = p.intValue();
                validatePositiveInteger(value, MAX_INPUT_TOKENS);
                return value;
            }, new ParseField(MAX_INPUT_TOKENS), ObjectParser.ValueType.INT_OR_NULL);
        }

        private StatefulValue<Integer> maxInputTokens = StatefulValue.undefined();

        public IbmWatsonxEmbeddingsServiceSettings mergeInto(IbmWatsonxEmbeddingsServiceSettings existing) {
            var updatedMaxInputTokens = applyUpdate(this.maxInputTokens, existing.maxInputTokens());
            return new IbmWatsonxEmbeddingsServiceSettings(
                existing.modelId(),
                existing.projectId(),
                existing.uri(),
                existing.apiVersion(),
                updatedMaxInputTokens,
                existing.dimensions(),
                existing.similarity(),
                mergedRateLimitSettings(existing)
            );
        }
    }
}
