/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings;

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
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

public abstract class BaseVoyageAIEmbeddingsServiceSettings extends FilteredXContentObject implements ServiceSettings {

    private static final TransportVersion VOYAGE_AI_INTEGRATION_ADDED = TransportVersion.fromName("voyage_ai_integration_added");

    @FunctionalInterface
    public interface ConstructorInvoker<T extends BaseVoyageAIEmbeddingsServiceSettings> {
        T construct(
            VoyageAIServiceSettings commonSettings,
            @Nullable VoyageAIEmbeddingType embeddingType,
            @Nullable SimilarityMeasure similarity,
            @Nullable Integer dimensions,
            @Nullable Integer maxInputTokens,
            boolean dimensionsSetByUser,
            boolean multimodalModel
        );
    }

    static <T extends BaseVoyageAIEmbeddingsServiceSettings> T fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        BiFunction<Map<String, Object>, ValidationException, Boolean> handleMultimodalModelField,
        ConstructorInvoker<T> constructor
    ) {
        ValidationException validationException = new ValidationException();
        var commonServiceSettings = VoyageAIServiceSettings.fromMap(map, context);

        VoyageAIEmbeddingType embeddingType = parseEmbeddingType(map, validationException);
        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dimensions = removeAsType(map, DIMENSIONS, Integer.class);
        Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);

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
            embeddingType,
            similarity,
            dimensions,
            maxInputTokens,
            dimensionsSetByUser,
            multimodalModel
        );
    }

    static VoyageAIEmbeddingType parseEmbeddingType(Map<String, Object> map, ValidationException validationException) {
        return Objects.requireNonNullElse(
            extractOptionalEnum(
                map,
                ServiceFields.EMBEDDING_TYPE,
                ModelConfigurations.SERVICE_SETTINGS,
                VoyageAIEmbeddingType::fromString,
                EnumSet.allOf(VoyageAIEmbeddingType.class),
                validationException
            ),
            VoyageAIEmbeddingType.FLOAT
        );
    }

    public static BaseVoyageAIEmbeddingsServiceSettings updateEmbeddingDetails(
        BaseVoyageAIEmbeddingsServiceSettings existingSettings,
        Integer embeddingSize,
        SimilarityMeasure similarityToUse
    ) {
        if (embeddingSize.equals(existingSettings.dimensions()) && similarityToUse.equals(existingSettings.similarity())) {
            return existingSettings;
        }
        return existingSettings.update(similarityToUse, embeddingSize);
    }

    private final VoyageAIServiceSettings commonSettings;
    private final VoyageAIEmbeddingType embeddingType;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final boolean dimensionsSetByUser;
    private final boolean multimodalModel;

    public BaseVoyageAIEmbeddingsServiceSettings(
        VoyageAIServiceSettings commonSettings,
        @Nullable VoyageAIEmbeddingType embeddingType,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        boolean dimensionsSetByUser,
        boolean multimodalModel
    ) {
        this.commonSettings = commonSettings;
        this.embeddingType = embeddingType != null ? embeddingType : VoyageAIEmbeddingType.FLOAT;
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.dimensionsSetByUser = dimensionsSetByUser;
        this.multimodalModel = multimodalModel;
    }

    public BaseVoyageAIEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new VoyageAIServiceSettings(in);
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.dimensions = in.readOptionalVInt();
        this.maxInputTokens = in.readOptionalVInt();
        this.embeddingType = Objects.requireNonNullElse(in.readOptionalEnum(VoyageAIEmbeddingType.class), VoyageAIEmbeddingType.FLOAT);
        this.dimensionsSetByUser = in.readBoolean();

        if (in.getTransportVersion().supports(VOYAGE_AI_INTEGRATION_ADDED)) {
            this.multimodalModel = in.readBoolean();
        } else {
            this.multimodalModel = false;
        }
    }

    /**
     * Returns a new settings instance with updated similarity and dimensions but all other fields unchanged
     * @param similarity the new similarity
     * @param dimensions the new dimensions
     * @return a new settings instance
     */
    public abstract BaseVoyageAIEmbeddingsServiceSettings update(SimilarityMeasure similarity, Integer dimensions);

    protected abstract void optionallyWriteMultimodalField(XContentBuilder builder) throws IOException;

    public VoyageAIServiceSettings getCommonSettings() {
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

    public VoyageAIEmbeddingType getEmbeddingType() {
        return embeddingType;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return embeddingType == null ? DenseVectorFieldMapper.ElementType.FLOAT : embeddingType.toElementType();
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

        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (embeddingType != null) {
            builder.field(ServiceFields.EMBEDDING_TYPE, embeddingType);
        }

        optionallyWriteMultimodalField(builder);

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return VOYAGE_AI_INTEGRATION_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(VOYAGE_AI_INTEGRATION_ADDED);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commonSettings.writeTo(out);
        out.writeOptionalEnum(similarity);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalEnum(embeddingType);
        out.writeBoolean(dimensionsSetByUser);

        if (out.getTransportVersion().supports(VOYAGE_AI_INTEGRATION_ADDED)) {
            out.writeBoolean(multimodalModel);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseVoyageAIEmbeddingsServiceSettings that = (BaseVoyageAIEmbeddingsServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings)
            && Objects.equals(embeddingType, that.embeddingType)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser)
            && Objects.equals(multimodalModel, that.multimodalModel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings, embeddingType, similarity, dimensions, maxInputTokens, dimensionsSetByUser, multimodalModel);
    }

    @Override
    public String toString() {
        return "BaseVoyageAIEmbeddingsServiceSettings{"
            + "commonSettings="
            + commonSettings
            + ", embeddingType="
            + embeddingType
            + ", similarity="
            + similarity
            + ", dimensions="
            + dimensions
            + ", maxInputTokens="
            + maxInputTokens
            + ", dimensionsSetByUser="
            + dimensionsSetByUser
            + ", multimodalModel="
            + multimodalModel
            + '}';
    }
}
