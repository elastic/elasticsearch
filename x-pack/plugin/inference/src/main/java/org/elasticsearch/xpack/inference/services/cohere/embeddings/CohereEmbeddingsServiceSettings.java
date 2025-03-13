/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;

public class CohereEmbeddingsServiceSettings extends FilteredXContentObject implements ServiceSettings {
    public static final String NAME = "cohere_embeddings_service_settings";

    static final String EMBEDDING_TYPE = "embedding_type";

    public static CohereEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();
        var commonServiceSettings = CohereServiceSettings.fromMap(map, context);

        CohereEmbeddingType embeddingTypes = parseEmbeddingType(map, context, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CohereEmbeddingsServiceSettings(commonServiceSettings, embeddingTypes);
    }

    static CohereEmbeddingType parseEmbeddingType(
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
                    CohereEmbeddingType::fromString,
                    EnumSet.allOf(CohereEmbeddingType.class),
                    validationException
                ),
                CohereEmbeddingType.FLOAT
            );
            case PERSISTENT -> {
                var embeddingType = ServiceUtils.extractOptionalString(
                    map,
                    EMBEDDING_TYPE,
                    ModelConfigurations.SERVICE_SETTINGS,
                    validationException
                );
                yield fromCohereOrDenseVectorEnumValues(embeddingType, validationException);
            }

        };
    }

    /**
     * Before TransportVersions::ML_INFERENCE_COHERE_EMBEDDINGS_ADDED element
     * type was persisted as a CohereEmbeddingType enum. After
     * DenseVectorFieldMapper.ElementType was used.
     *
     * Parse either and convert to a CohereEmbeddingType
     */
    static CohereEmbeddingType fromCohereOrDenseVectorEnumValues(String enumString, ValidationException validationException) {
        if (enumString == null) {
            return CohereEmbeddingType.FLOAT;
        }

        try {
            return CohereEmbeddingType.fromString(enumString);
        } catch (IllegalArgumentException ae) {
            try {
                return CohereEmbeddingType.fromElementType(DenseVectorFieldMapper.ElementType.fromString(enumString));
            } catch (IllegalArgumentException iae) {
                var validValuesAsStrings = CohereEmbeddingType.SUPPORTED_ELEMENT_TYPES.stream()
                    .map(value -> value.toString().toLowerCase(Locale.ROOT))
                    .toArray(String[]::new);
                validationException.addValidationError(
                    ServiceUtils.invalidValue(EMBEDDING_TYPE, ModelConfigurations.SERVICE_SETTINGS, enumString, validValuesAsStrings)
                );
                return null;
            }
        }
    }

    private final CohereServiceSettings commonSettings;
    private final CohereEmbeddingType embeddingType;

    public CohereEmbeddingsServiceSettings(CohereServiceSettings commonSettings, CohereEmbeddingType embeddingType) {
        this.commonSettings = commonSettings;
        this.embeddingType = Objects.requireNonNull(embeddingType);
    }

    public CohereEmbeddingsServiceSettings(StreamInput in) throws IOException {
        commonSettings = new CohereServiceSettings(in);
        embeddingType = Objects.requireNonNullElse(in.readOptionalEnum(CohereEmbeddingType.class), CohereEmbeddingType.FLOAT);
    }

    public CohereServiceSettings getCommonSettings() {
        return commonSettings;
    }

    @Override
    public SimilarityMeasure similarity() {
        return commonSettings.similarity();
    }

    @Override
    public Integer dimensions() {
        return commonSettings.dimensions();
    }

    @Override
    public String modelId() {
        return commonSettings.modelId();
    }

    public CohereEmbeddingType getEmbeddingType() {
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

        commonSettings.toXContentFragment(builder, params);
        builder.field(EMBEDDING_TYPE, elementType());

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        commonSettings.toXContentFragmentOfExposedFields(builder, params);
        builder.field(EMBEDDING_TYPE, elementType());

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_13_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commonSettings.writeTo(out);
        out.writeOptionalEnum(CohereEmbeddingType.translateToVersion(embeddingType, out.getTransportVersion()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CohereEmbeddingsServiceSettings that = (CohereEmbeddingsServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings) && Objects.equals(embeddingType, that.embeddingType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings, embeddingType);
    }
}
