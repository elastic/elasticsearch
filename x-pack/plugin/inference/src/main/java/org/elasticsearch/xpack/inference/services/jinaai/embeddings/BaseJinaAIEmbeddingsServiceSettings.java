/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.parser.EnumParser;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;

import static org.elasticsearch.inference.EmbeddingRequest.JINA_AI_EMBEDDING_TASK_ADDED;
import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.EMBEDDING_TYPE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;

/**
 * Abstract base for the JinaAI embeddings service settings. Extends {@link JinaAIServiceSettings} and adds the embeddings-specific
 * fields (similarity, dimensions, max input tokens, embedding type). The two concrete embeddings variants (text and multimodal)
 * differ only in whether they carry a {@code multimodal_model} flag.
 */
public abstract class BaseJinaAIEmbeddingsServiceSettings extends JinaAIServiceSettings {

    static final TransportVersion JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED = TransportVersion.fromName("jina_ai_embedding_type_support_added");

    static final TransportVersion JINA_AI_EMBEDDING_DIMENSIONS_SUPPORT_ADDED = TransportVersion.fromName(
        "jina_ai_embedding_dimensions_support_added"
    );

    /**
     * Registers the embeddings-specific fields (similarity, dimensions, max_input_tokens, embedding_type) onto the given parser. The
     * internal {@code dimensions_set_by_user} field is only declared for {@link ConfigurationParseContext#PERSISTENT} parsing; in a
     * request it is derived from whether {@code dimensions} was supplied.
     */
    public static <B extends Builder<?>> void declareEmbeddingFields(
        AbstractObjectParser<B, ConfigurationParseContext> parser,
        ConfigurationParseContext context
    ) {
        parser.declareString(Builder::setSimilarity, EnumParser::parseSimilarity, new ParseField(SIMILARITY));
        parser.declareInt(Builder::setDimensions, new ParseField(DIMENSIONS));
        parser.declareInt(Builder::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
        parser.declareString(
            Builder::setEmbeddingType,
            BaseJinaAIEmbeddingsServiceSettings::parseEmbeddingType,
            new ParseField(EMBEDDING_TYPE)
        );
        if (context == ConfigurationParseContext.PERSISTENT) {
            parser.declareBoolean(Builder::setDimensionsSetByUser, new ParseField(DIMENSIONS_SET_BY_USER));
        }
    }

    static JinaAIEmbeddingType parseEmbeddingType(String value) {
        return EnumParser.parseFromStringInObjectParserContext(
            value,
            JinaAIEmbeddingType::fromString,
            EnumSet.allOf(JinaAIEmbeddingType.class),
            EnumSet.noneOf(JinaAIEmbeddingType.class)
        );
    }

    public static BaseJinaAIEmbeddingsServiceSettings updateEmbeddingDetails(
        BaseJinaAIEmbeddingsServiceSettings existingSettings,
        Integer embeddingSize,
        SimilarityMeasure similarityToUse
    ) {
        if (embeddingSize.equals(existingSettings.dimensions()) && similarityToUse.equals(existingSettings.similarity())) {
            return existingSettings;
        }
        return existingSettings.update(similarityToUse, embeddingSize);
    }

    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final JinaAIEmbeddingType embeddingType;
    private final boolean dimensionsSetByUser;
    private final boolean multimodalModel;

    protected BaseJinaAIEmbeddingsServiceSettings(
        String modelId,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable JinaAIEmbeddingType embeddingType,
        boolean dimensionsSetByUser,
        boolean multimodalModel,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        super(modelId, rateLimitSettings);
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.embeddingType = embeddingType != null ? embeddingType : JinaAIEmbeddingType.FLOAT;
        this.dimensionsSetByUser = dimensionsSetByUser;
        this.multimodalModel = multimodalModel;
    }

    protected BaseJinaAIEmbeddingsServiceSettings(StreamInput in) throws IOException {
        super(in);
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.dimensions = in.readOptionalVInt();
        this.maxInputTokens = in.readOptionalVInt();
        if (in.getTransportVersion().supports(JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED)) {
            this.embeddingType = Objects.requireNonNullElse(in.readOptionalEnum(JinaAIEmbeddingType.class), JinaAIEmbeddingType.FLOAT);
        } else {
            this.embeddingType = JinaAIEmbeddingType.FLOAT;
        }

        if (in.getTransportVersion().supports(JINA_AI_EMBEDDING_DIMENSIONS_SUPPORT_ADDED)) {
            this.dimensionsSetByUser = in.readBoolean();
        } else {
            this.dimensionsSetByUser = false;
        }

        if (in.getTransportVersion().supports(JINA_AI_EMBEDDING_TASK_ADDED)) {
            this.multimodalModel = in.readBoolean();
        } else {
            this.multimodalModel = false;
        }
    }

    /**
     * Returns a new {@link BaseJinaAIEmbeddingsServiceSettings} with updated similarity and dimensions but all other fields unchanged
     * @param similarity the new similarity
     * @param dimensions the new dimensions
     * @return a new {@link BaseJinaAIEmbeddingsServiceSettings}
     */
    public abstract BaseJinaAIEmbeddingsServiceSettings update(SimilarityMeasure similarity, Integer dimensions);

    protected abstract void optionallyWriteMultimodalField(XContentBuilder builder) throws IOException;

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
        return dimensionsSetByUser;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    public JinaAIEmbeddingType getEmbeddingType() {
        return embeddingType;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return embeddingType.toElementType();
    }

    @Override
    public boolean isMultimodal() {
        return multimodalModel;
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
        super.toXContentFragmentOfExposedFields(builder, params);
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }

        builder.field(EMBEDDING_TYPE, embeddingType);

        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }

        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }

        optionallyWriteMultimodalField(builder);

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalEnum(similarity);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
        if (out.getTransportVersion().supports(JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED)) {
            out.writeOptionalEnum(JinaAIEmbeddingType.translateToVersion(embeddingType, out.getTransportVersion()));
        }

        if (out.getTransportVersion().supports(JINA_AI_EMBEDDING_DIMENSIONS_SUPPORT_ADDED)) {
            out.writeBoolean(dimensionsSetByUser);
        }

        if (out.getTransportVersion().supports(JINA_AI_EMBEDDING_TASK_ADDED)) {
            out.writeOptionalBoolean(multimodalModel);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;
        BaseJinaAIEmbeddingsServiceSettings that = (BaseJinaAIEmbeddingsServiceSettings) o;
        return Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(embeddingType, that.embeddingType)
            && dimensionsSetByUser == that.dimensionsSetByUser
            && multimodalModel == that.multimodalModel;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), similarity, dimensions, maxInputTokens, embeddingType, dimensionsSetByUser, multimodalModel);
    }

    /**
     * Accumulates the embeddings-specific fields on top of the common JinaAI fields. Concrete subclasses provide a {@link #construct}
     * implementation that produces their own settings type and supplies the task-specific {@code multimodalModel} value. The {@code
     * dimensions_set_by_user} flag is resolved from the captured {@link ConfigurationParseContext}.
     *
     * @param <T> the task-specific settings type
     */
    public abstract static class Builder<T extends BaseJinaAIEmbeddingsServiceSettings> extends JinaAIServiceSettings.Builder<T> {

        private SimilarityMeasure similarity;
        private Integer dimensions;
        private Integer maxInputTokens;
        private JinaAIEmbeddingType embeddingType;
        private Boolean dimensionsSetByUser;
        protected boolean multimodalModel;

        protected Builder(ConfigurationParseContext context) {
            super(context);
        }

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

        public void setEmbeddingType(JinaAIEmbeddingType embeddingType) {
            this.embeddingType = embeddingType;
        }

        public void setDimensionsSetByUser(boolean dimensionsSetByUser) {
            this.dimensionsSetByUser = dimensionsSetByUser;
        }

        public void setMultimodalModel(boolean multimodalModel) {
            this.multimodalModel = multimodalModel;
        }

        protected abstract T construct(
            String modelId,
            @Nullable SimilarityMeasure similarity,
            @Nullable Integer dimensions,
            @Nullable Integer maxInputTokens,
            @Nullable JinaAIEmbeddingType embeddingType,
            boolean dimensionsSetByUser,
            @Nullable RateLimitSettings rateLimitSettings
        );

        @Override
        protected final T build(String modelId, RateLimitSettings rateLimitSettings) {
            // In a request the flag is derived from whether dimensions were provided; in a persisted config it is read back from the
            // stored value, defaulting to false when absent.
            boolean resolvedDimensionsSetByUser = context == ConfigurationParseContext.PERSISTENT
                ? Boolean.TRUE.equals(dimensionsSetByUser)
                : dimensions != null;
            return construct(
                modelId,
                similarity,
                dimensions,
                maxInputTokens,
                embeddingType,
                resolvedDimensionsSetByUser,
                rateLimitSettings
            );
        }
    }

    /**
     * Common fields parsed from an embeddings update request. In addition to the mutable {@code rate_limit} inherited from {@link
     * JinaAIServiceSettings.CommonUpdate}, {@code max_input_tokens} may also be changed or cleared.
     */
    public static class EmbeddingsUpdate extends JinaAIServiceSettings.CommonUpdate {

        protected StatefulValue<Integer> maxInputTokens = StatefulValue.undefined();
    }

    /**
     * Registers the embeddings fields that may be changed by an update request ({@code rate_limit} and {@code max_input_tokens}).
     * All other fields are intentionally not declared so that a strict update parser rejects attempts to change them.
     */
    public static void declareEmbeddingsUpdatableFields(AbstractObjectParser<? extends EmbeddingsUpdate, Void> parser) {
        JinaAIServiceSettings.declareCommonUpdatableFields(parser);
        StatefulValue.declareNullable(parser, (update, value) -> update.maxInputTokens = value, p -> {
            Integer value = p.intValue();
            validatePositiveInteger(value, MAX_INPUT_TOKENS);
            return value;
        }, new ParseField(MAX_INPUT_TOKENS), ObjectParser.ValueType.INT_OR_NULL);
    }
}
