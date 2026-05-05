/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIService;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS_SET_BY_USER;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;

public class VoyageAIEmbeddingsServiceSettings extends VoyageAIServiceSettings {
    public static final String NAME = "voyageai_embeddings_service_settings";

    private static final TransportVersion VOYAGE_AI_INTEGRATION_ADDED = TransportVersion.fromName("voyage_ai_integration_added");
    private static final TransportVersion VOYAGE_AI_EMBEDDING_TYPE_NON_NULLABLE = TransportVersion.fromName(
        "voyage_ai_embedding_type_non_nullable"
    );

    public static VoyageAIEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var modelId = extractRequiredString(map, ServiceFields.MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var rateLimitSettings = extractRateLimitSettings(map, context, validationException);

        var embeddingType = Objects.requireNonNullElse(
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

        var similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        var dimensionsSetByUser = extractOptionalBoolean(map, DIMENSIONS_SET_BY_USER, validationException);

        switch (context) {
            case REQUEST -> {
                if (dimensionsSetByUser != null) {
                    validationException.addValidationError(
                        ServiceUtils.invalidSettingError(ServiceFields.DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
                dimensionsSetByUser = dimensions != null;
            }
            case PERSISTENT -> {
                if (dimensionsSetByUser == null) {
                    dimensionsSetByUser = false;
                }
            }
        }

        validationException.throwIfValidationErrorsExist();

        return new VoyageAIEmbeddingsServiceSettings(
            modelId,
            rateLimitSettings,
            embeddingType,
            similarity,
            dimensions,
            maxInputTokens,
            dimensionsSetByUser
        );
    }

    private final VoyageAIEmbeddingType embeddingType;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final boolean dimensionsSetByUser;

    public VoyageAIEmbeddingsServiceSettings(
        String modelId,
        @Nullable RateLimitSettings rateLimitSettings,
        VoyageAIEmbeddingType embeddingType,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        boolean dimensionsSetByUser
    ) {
        super(modelId, rateLimitSettings);
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.embeddingType = Objects.requireNonNull(embeddingType);
        this.dimensionsSetByUser = dimensionsSetByUser;
    }

    public VoyageAIEmbeddingsServiceSettings(StreamInput in) throws IOException {
        super(in);
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.dimensions = in.readOptionalVInt();
        this.maxInputTokens = in.readOptionalVInt();
        if (VOYAGE_AI_EMBEDDING_TYPE_NON_NULLABLE.supports(in.getTransportVersion())) {
            this.embeddingType = in.readEnum(VoyageAIEmbeddingType.class);
        } else {
            this.embeddingType = Objects.requireNonNullElse(in.readOptionalEnum(VoyageAIEmbeddingType.class), VoyageAIEmbeddingType.FLOAT);
        }
        this.dimensionsSetByUser = in.readBoolean();
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public VoyageAIEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
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
            VoyageAIService.NAME,
            ConfigurationParseContext.REQUEST
        );

        validationException.throwIfValidationErrorsExist();

        return new VoyageAIEmbeddingsServiceSettings(
            this.modelId(),
            extractedRateLimitSettings,
            this.embeddingType,
            this.similarity,
            this.dimensions,
            extractedMaxInputTokens != null ? extractedMaxInputTokens : this.maxInputTokens,
            this.dimensionsSetByUser
        );
    }

    public VoyageAIEmbeddingType embeddingType() {
        return embeddingType;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return embeddingType.toElementType();
    }

    @Override
    public Boolean dimensionsSetByUser() {
        return this.dimensionsSetByUser;
    }

    @Override
    public String getWriteableName() {
        return NAME;
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
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        builder.field(ServiceFields.EMBEDDING_TYPE, embeddingType);
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
        super.writeTo(out);
        out.writeOptionalEnum(similarity);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
        if (VOYAGE_AI_EMBEDDING_TYPE_NON_NULLABLE.supports(out.getTransportVersion())) {
            out.writeEnum(embeddingType);
        } else {
            out.writeOptionalEnum(embeddingType);
        }
        out.writeBoolean(dimensionsSetByUser);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VoyageAIEmbeddingsServiceSettings that = (VoyageAIEmbeddingsServiceSettings) o;
        return Objects.equals(modelId(), that.modelId())
            && Objects.equals(rateLimitSettings(), that.rateLimitSettings())
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(embeddingType, that.embeddingType)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId(), rateLimitSettings(), similarity, dimensions, maxInputTokens, embeddingType, dimensionsSetByUser);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
