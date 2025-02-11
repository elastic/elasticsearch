/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

public class VoyageAIEmbeddingsServiceSettings extends FilteredXContentObject implements ServiceSettings {
    public static final String NAME = "voyageai_embeddings_service_settings";
    public static final VoyageAIEmbeddingsServiceSettings EMPTY_SETTINGS = new VoyageAIEmbeddingsServiceSettings(
        null,
        null,
        null,
        null,
        null
    );

    public static final String EMBEDDING_TYPE = "embedding_type";

    public static VoyageAIEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();
        var commonServiceSettings = VoyageAIServiceSettings.fromMap(map, context);

        VoyageAIEmbeddingType embeddingTypes = parseEmbeddingType(map, context, validationException);

        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dims = removeAsType(map, DIMENSIONS, Integer.class);
        Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new VoyageAIEmbeddingsServiceSettings(commonServiceSettings, embeddingTypes, similarity, dims, maxInputTokens);
    }

    static VoyageAIEmbeddingType parseEmbeddingType(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        return switch (context) {
            case REQUEST -> Objects.requireNonNullElse(
                extractOptionalEnum(
                    map,
                    EMBEDDING_TYPE,
                    ModelConfigurations.SERVICE_SETTINGS,
                    VoyageAIEmbeddingType::fromString,
                    EnumSet.allOf(VoyageAIEmbeddingType.class),
                    validationException
                ),
                VoyageAIEmbeddingType.FLOAT
            );
            case PERSISTENT -> {
                var embeddingType = ServiceUtils.extractOptionalString(
                    map,
                    EMBEDDING_TYPE,
                    ModelConfigurations.SERVICE_SETTINGS,
                    validationException
                );
                yield fromVoyageAIOrDenseVectorEnumValues(embeddingType, validationException);
            }

        };
    }

    static VoyageAIEmbeddingType fromVoyageAIOrDenseVectorEnumValues(String enumString, ValidationException validationException) {
        if (enumString == null) {
            return VoyageAIEmbeddingType.FLOAT;
        }

        try {
            return VoyageAIEmbeddingType.fromString(enumString);
        } catch (IllegalArgumentException ae) {
            try {
                return VoyageAIEmbeddingType.fromElementType(DenseVectorFieldMapper.ElementType.fromString(enumString));
            } catch (IllegalArgumentException iae) {
                var validValuesAsStrings = VoyageAIEmbeddingType.SUPPORTED_ELEMENT_TYPES.stream()
                    .map(value -> value.toString().toLowerCase(Locale.ROOT))
                    .toArray(String[]::new);
                validationException.addValidationError(
                    ServiceUtils.invalidValue(EMBEDDING_TYPE, ModelConfigurations.SERVICE_SETTINGS, enumString, validValuesAsStrings)
                );
                return null;
            }
        }
    }

    private final VoyageAIServiceSettings commonSettings;
    private final VoyageAIEmbeddingType embeddingType;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;

    public VoyageAIEmbeddingsServiceSettings(
        VoyageAIServiceSettings commonSettings,
        @Nullable VoyageAIEmbeddingType embeddingType,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens
    ) {
        this.commonSettings = commonSettings;
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.embeddingType = embeddingType;
    }

    public VoyageAIEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new VoyageAIServiceSettings(in);
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.dimensions = in.readOptionalVInt();
        this.maxInputTokens = in.readOptionalVInt();
        this.embeddingType = Objects.requireNonNullElse(in.readOptionalEnum(VoyageAIEmbeddingType.class), VoyageAIEmbeddingType.FLOAT);
    }

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
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder = commonSettings.toXContentFragment(builder, params);
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
            builder.field(EMBEDDING_TYPE, embeddingType);
        }
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        commonSettings.toXContentFragmentOfExposedFields(builder, params);

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.VOYAGE_AI_INTEGRATION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commonSettings.writeTo(out);
        out.writeOptionalEnum(SimilarityMeasure.translateSimilarity(similarity, out.getTransportVersion()));
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalEnum(embeddingType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VoyageAIEmbeddingsServiceSettings that = (VoyageAIEmbeddingsServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(embeddingType, that.embeddingType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings, similarity, dimensions, maxInputTokens, embeddingType);
    }
}
