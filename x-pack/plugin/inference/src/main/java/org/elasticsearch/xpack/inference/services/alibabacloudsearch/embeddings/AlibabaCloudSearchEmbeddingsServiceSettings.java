/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.EnumParser;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveInteger;
import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;

/**
 * Settings for the AlibabaCloud AI Search text embeddings task. Wraps the {@link AlibabaCloudSearchServiceSettings} common to every
 * AlibabaCloud AI Search task and adds the embeddings-specific fields: similarity measure, dimensions, and max input tokens.
 */
public class AlibabaCloudSearchEmbeddingsServiceSettings implements ServiceSettings {
    public static final String NAME = "alibabacloud_search_embeddings_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * Creates an {@link ObjectParser} for the AlibabaCloud AI Search embeddings service settings.
     *
     * @param ignoreUnknownFields whether the parser should tolerate unknown fields. This is {@code false} for request parsing (so that
     *                            unexpected fields are rejected) and {@code true} for persisted configuration (so that fields written by
     *                            other versions are tolerated).
     * @return the parser
     */
    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields) {
        var parser = AlibabaCloudSearchServiceSettings.buildCommonParser(ignoreUnknownFields, Builder::new);
        parser.declareString(Builder::setSimilarity, EnumParser::parseSimilarity, new ParseField(SIMILARITY));
        parser.declareInt(Builder::setDimensions, new ParseField(DIMENSIONS));
        parser.declareInt(Builder::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
        return parser;
    }

    public static AlibabaCloudSearchEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return AlibabaCloudSearchServiceSettings.fromMap(map, context, parser);
    }

    /**
     * Accumulates the embeddings-specific fields on top of the common AlibabaCloud AI Search fields and builds an
     * {@link AlibabaCloudSearchEmbeddingsServiceSettings}, enforcing that {@code dimensions} and {@code max_input_tokens} are positive.
     */
    public static class Builder extends AlibabaCloudSearchServiceSettings.Builder<AlibabaCloudSearchEmbeddingsServiceSettings> {
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

        @Override
        protected AlibabaCloudSearchEmbeddingsServiceSettings build(AlibabaCloudSearchServiceSettings commonSettings) {
            return new AlibabaCloudSearchEmbeddingsServiceSettings(commonSettings, similarity, dimensions, maxInputTokens);
        }
    }

    private final AlibabaCloudSearchServiceSettings commonSettings;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;

    public AlibabaCloudSearchEmbeddingsServiceSettings(
        AlibabaCloudSearchServiceSettings commonSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens
    ) {
        this.commonSettings = commonSettings;
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
    }

    public AlibabaCloudSearchEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new AlibabaCloudSearchServiceSettings(in);
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.dimensions = in.readOptionalVInt();
        this.maxInputTokens = in.readOptionalVInt();
    }

    public AlibabaCloudSearchServiceSettings getCommonSettings() {
        return commonSettings;
    }

    public SimilarityMeasure getSimilarity() {
        return similarity;
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

    public Integer getMaxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public String modelId() {
        return commonSettings.modelId();
    }

    @Override
    public AlibabaCloudSearchEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse AlibabaCloud AI Search embeddings service settings update", e);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code http_schema}, {@code rate_limit} and {@code max_input_tokens}
     * fields. Including any immutable field (such as {@code service_id}, {@code host}, {@code workspace}, {@code similarity} or
     * {@code dimensions}) causes the strict parser to reject the request.
     */
    private static class Update extends AlibabaCloudSearchServiceSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = createUpdateParser();

        private static ObjectParser<Update, Void> createUpdateParser() {
            var parser = AlibabaCloudSearchServiceSettings.buildCommonUpdateParser(Update::new);
            StatefulValue.declareNullable(parser, (update, value) -> update.maxInputTokens = value, p -> {
                Integer value = p.intValue();
                validatePositiveInteger(value, MAX_INPUT_TOKENS);
                return value;
            }, new ParseField(MAX_INPUT_TOKENS), ObjectParser.ValueType.INT_OR_NULL);
            return parser;
        }

        private StatefulValue<Integer> maxInputTokens = StatefulValue.undefined();

        public AlibabaCloudSearchEmbeddingsServiceSettings mergeInto(AlibabaCloudSearchEmbeddingsServiceSettings existing) {
            return new AlibabaCloudSearchEmbeddingsServiceSettings(
                mergedCommonSettings(existing.getCommonSettings()),
                existing.similarity(),
                existing.dimensions(),
                applyUpdate(maxInputTokens, existing.getMaxInputTokens())
            );
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        commonSettings.toXContentFragment(builder, params);
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commonSettings.writeTo(out);
        out.writeOptionalEnum(similarity);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlibabaCloudSearchEmbeddingsServiceSettings that = (AlibabaCloudSearchEmbeddingsServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings, similarity, dimensions, maxInputTokens);
    }
}
