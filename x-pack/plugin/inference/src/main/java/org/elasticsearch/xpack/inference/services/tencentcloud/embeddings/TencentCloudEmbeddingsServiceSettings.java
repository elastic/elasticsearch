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
 * Settings for the TencentCloud embeddings service. Extends {@link TencentCloudCommonServiceSettings} and adds the
 * embeddings-specific fields: dimensions, similarity measure, and max input tokens.
 */
public class TencentCloudEmbeddingsServiceSettings extends TencentCloudCommonServiceSettings {

    public static final String NAME = "tencentcloud_embeddings_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean isPersistentParser) {
        ObjectParser<Builder, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            isPersistentParser,
            Builder::new
        );
        TencentCloudCommonServiceSettings.declareCommonFields(parser, TencentCloudCommonServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS);
        parser.declareString(Builder::setSimilarity, SimilarityMeasure::fromString, new ParseField(SIMILARITY));
        // The Tencent Cloud embeddings API does not accept a `dimensions` parameter, so only parse it from the persistent parser where it
        // configures the local index mapping rather than the outbound request.
        if (isPersistentParser) {
            parser.declareInt(Builder::setDimensions, new ParseField(DIMENSIONS));
        }
        parser.declareInt(Builder::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
        return parser;
    }

    public static TencentCloudEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return TencentCloudCommonServiceSettings.fromMap(map, context, parser);
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
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse TencentCloud embeddings service settings update", e);
        }
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
            validatePositiveInteger(dimensions, DIMENSIONS);
            this.dimensions = dimensions;
        }

        public void setMaxInputTokens(Integer maxInputTokens) {
            validatePositiveInteger(maxInputTokens, MAX_INPUT_TOKENS);
            this.maxInputTokens = maxInputTokens;
        }

        @Override
        protected TencentCloudEmbeddingsServiceSettings build(String modelId, String region, RateLimitSettings rateLimitSettings) {
            return new TencentCloudEmbeddingsServiceSettings(modelId, region, rateLimitSettings, similarity, dimensions, maxInputTokens);
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

        public TencentCloudEmbeddingsServiceSettings mergeInto(TencentCloudEmbeddingsServiceSettings existing) {
            var updatedMaxInputTokens = applyUpdate(this.maxInputTokens, existing.maxInputTokens());
            return new TencentCloudEmbeddingsServiceSettings(
                existing.modelId(),
                existing.region(),
                mergedRateLimitSettings(existing),
                existing.similarity(),
                existing.dimensions(),
                updatedMaxInputTokens
            );
        }
    }
}
