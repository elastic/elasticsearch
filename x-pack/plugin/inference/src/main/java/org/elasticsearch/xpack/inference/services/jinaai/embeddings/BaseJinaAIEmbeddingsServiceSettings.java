/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

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
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.elasticsearch.inference.EmbeddingRequest.JINA_AI_EMBEDDING_TASK_ADDED;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.EMBEDDING_TYPE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

public abstract class BaseJinaAIEmbeddingsServiceSettings extends FilteredXContentObject implements ServiceSettings {

    static final TransportVersion JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED = TransportVersion.fromName("jina_ai_embedding_type_support_added");

    static final TransportVersion JINA_AI_EMBEDDING_DIMENSIONS_SUPPORT_ADDED = TransportVersion.fromName(
        "jina_ai_embedding_dimensions_support_added"
    );

    @FunctionalInterface
    public interface ConstructorInvoker<T extends BaseJinaAIEmbeddingsServiceSettings> {
        T construct(
            JinaAIServiceSettings commonSettings,
            @Nullable SimilarityMeasure similarity,
            @Nullable Integer dimensions,
            @Nullable Integer maxInputTokens,
            @Nullable JinaAIEmbeddingType embeddingType,
            boolean dimensionsSetByUser,
            boolean multimodalModel
        );
    }

    static <T extends BaseJinaAIEmbeddingsServiceSettings> T fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        BiFunction<Map<String, Object>, ValidationException, Boolean> handleMultimodalModelField,
        ConstructorInvoker<T> constructor
    ) {
        ValidationException validationException = new ValidationException();
        var commonServiceSettings = JinaAIServiceSettings.fromMap(map, context);
        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class, validationException);

        JinaAIEmbeddingType embeddingType = parseEmbeddingType(map, validationException);

        Boolean dimensionsSetByUser;
        if (context == ConfigurationParseContext.PERSISTENT) {
            dimensionsSetByUser = removeAsType(map, DIMENSIONS_SET_BY_USER, Boolean.class, validationException);
            if (dimensionsSetByUser == null) {
                dimensionsSetByUser = Boolean.FALSE;
            }
        } else {
            dimensionsSetByUser = dimensions != null;
        }

        boolean multimodalModel = handleMultimodalModelField.apply(map, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return constructor.construct(
            commonServiceSettings,
            similarity,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser,
            multimodalModel
        );
    }

    static JinaAIEmbeddingType parseEmbeddingType(Map<String, Object> map, ValidationException validationException) {
        return Objects.requireNonNullElse(
            extractOptionalEnum(
                map,
                EMBEDDING_TYPE,
                ModelConfigurations.SERVICE_SETTINGS,
                JinaAIEmbeddingType::fromString,
                EnumSet.allOf(JinaAIEmbeddingType.class),
                validationException
            ),
            JinaAIEmbeddingType.FLOAT
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

    private final JinaAIServiceSettings commonSettings;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final JinaAIEmbeddingType embeddingType;
    private final boolean dimensionsSetByUser;
    private final boolean multimodalModel;

    public BaseJinaAIEmbeddingsServiceSettings(
        JinaAIServiceSettings commonSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable JinaAIEmbeddingType embeddingType,
        boolean dimensionsSetByUser,
        boolean multimodalModel
    ) {
        this.commonSettings = commonSettings;
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.embeddingType = embeddingType != null ? embeddingType : JinaAIEmbeddingType.FLOAT;
        this.dimensionsSetByUser = dimensionsSetByUser;
        this.multimodalModel = multimodalModel;
    }

    public BaseJinaAIEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new JinaAIServiceSettings(in);
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

    public JinaAIServiceSettings getCommonSettings() {
        return commonSettings;
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
        return dimensionsSetByUser;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public String modelId() {
        return commonSettings.modelId();
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
        commonSettings.toXContentFragmentOfExposedFields(builder, params);
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }

        builder.field(ServiceFields.EMBEDDING_TYPE, embeddingType);

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
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commonSettings.writeTo(out);
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
        if (o == null || getClass() != o.getClass()) return false;
        BaseJinaAIEmbeddingsServiceSettings that = (BaseJinaAIEmbeddingsServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(embeddingType, that.embeddingType)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser)
            && Objects.equals(multimodalModel, that.multimodalModel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings, similarity, dimensions, maxInputTokens, embeddingType, dimensionsSetByUser, multimodalModel);
    }

    @Override
    public String toString() {
        return "BaseJinaAIEmbeddingsServiceSettings{"
            + "commonSettings="
            + commonSettings
            + ", similarity="
            + similarity
            + ", dimensions="
            + dimensions
            + ", maxInputTokens="
            + maxInputTokens
            + ", embeddingType="
            + embeddingType
            + ", dimensionsSetByUser="
            + dimensionsSetByUser
            + ", multimodalModel="
            + multimodalModel
            + '}';
    }
}
