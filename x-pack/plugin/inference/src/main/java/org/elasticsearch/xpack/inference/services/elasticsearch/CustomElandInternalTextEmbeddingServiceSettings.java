/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.ELEMENT_TYPE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;

public class CustomElandInternalTextEmbeddingServiceSettings extends ElasticsearchInternalServiceSettings {

    public static final String NAME = "custom_eland_model_internal_text_embedding_service_settings";

    /**
     * Parse the CustomElandServiceSettings from map and validate the setting values.
     *
     * This method does not verify the model variant
     *
     * If required setting are missing or the values are invalid an
     * {@link ValidationException} is thrown.
     *
     * @param map Source map containing the config
     * @param context The parser context, whether it is from an HTTP request or from persistent storage
     * @return The {@code CustomElandServiceSettings} builder
     */
    public static CustomElandInternalTextEmbeddingServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return switch (context) {
            case REQUEST -> forRequest(map);
            case PERSISTENT -> forPersisted(map);
        };
    }

    private static CustomElandInternalTextEmbeddingServiceSettings forRequest(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        var commonFields = commonFieldsFromMap(map, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CustomElandInternalTextEmbeddingServiceSettings(commonFields);
    }

    private static CustomElandInternalTextEmbeddingServiceSettings forPersisted(Map<String, Object> map) {
        var commonFields = commonFieldsFromMap(map);
        Integer dims = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, new ValidationException());

        return new CustomElandInternalTextEmbeddingServiceSettings(commonFields, dims);
    }

    private record CommonFields(
        ElasticsearchInternalServiceSettings internalServiceSettings,
        SimilarityMeasure similarityMeasure,
        DenseVectorFieldMapper.ElementType elementType
    ) {}

    private static CommonFields commonFieldsFromMap(Map<String, Object> map) {
        return commonFieldsFromMap(map, new ValidationException());
    }

    private static CommonFields commonFieldsFromMap(Map<String, Object> map, ValidationException validationException) {
        var internalSettings = ElasticsearchInternalServiceSettings.fromMap(map, validationException);
        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        DenseVectorFieldMapper.ElementType elementType = extractOptionalEnum(
            map,
            ELEMENT_TYPE,
            ModelConfigurations.SERVICE_SETTINGS,
            DenseVectorFieldMapper.ElementType::fromString,
            EnumSet.of(DenseVectorFieldMapper.ElementType.BYTE, DenseVectorFieldMapper.ElementType.FLOAT),
            validationException
        );

        return new CommonFields(
            internalSettings.build(),
            Objects.requireNonNullElse(similarity, SimilarityMeasure.COSINE),
            Objects.requireNonNullElse(elementType, DenseVectorFieldMapper.ElementType.FLOAT)
        );
    }

    private final Integer dimensions;
    private final SimilarityMeasure similarityMeasure;
    private final DenseVectorFieldMapper.ElementType elementType;

    public CustomElandInternalTextEmbeddingServiceSettings(
        int numAllocations,
        int numThreads,
        String modelId,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        this(
            numAllocations,
            numThreads,
            modelId,
            adaptiveAllocationsSettings,
            null,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT
        );
    }

    public CustomElandInternalTextEmbeddingServiceSettings(
        int numAllocations,
        int numThreads,
        String modelId,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        Integer dimensions,
        SimilarityMeasure similarityMeasure,
        DenseVectorFieldMapper.ElementType elementType
    ) {
        super(numAllocations, numThreads, modelId, adaptiveAllocationsSettings);
        this.dimensions = dimensions;
        this.similarityMeasure = Objects.requireNonNull(similarityMeasure);
        this.elementType = Objects.requireNonNull(elementType);
    }

    public CustomElandInternalTextEmbeddingServiceSettings(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            dimensions = in.readOptionalVInt();
            similarityMeasure = in.readEnum(SimilarityMeasure.class);
            elementType = in.readEnum(DenseVectorFieldMapper.ElementType.class);
        } else {
            dimensions = null;
            similarityMeasure = SimilarityMeasure.COSINE;
            elementType = DenseVectorFieldMapper.ElementType.FLOAT;
        }
    }

    private CustomElandInternalTextEmbeddingServiceSettings(CommonFields commonFields) {
        this(commonFields, null);
    }

    private CustomElandInternalTextEmbeddingServiceSettings(CommonFields commonFields, Integer dimensions) {
        super(
            commonFields.internalServiceSettings.getNumAllocations(),
            commonFields.internalServiceSettings.getNumThreads(),
            commonFields.internalServiceSettings.modelId(),
            commonFields.internalServiceSettings.getAdaptiveAllocationsSettings()
        );
        this.dimensions = dimensions;
        similarityMeasure = commonFields.similarityMeasure;
        elementType = commonFields.elementType;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        addInternalSettingsToXContent(builder, params);

        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }

        if (similarityMeasure != null) {
            builder.field(SIMILARITY, similarityMeasure);
        }

        if (elementType != null) {
            builder.field(ELEMENT_TYPE, elementType);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return CustomElandInternalTextEmbeddingServiceSettings.NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeOptionalVInt(dimensions);
            out.writeEnum(similarityMeasure);
            out.writeEnum(elementType);
        }
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return elementType;
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarityMeasure;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomElandInternalTextEmbeddingServiceSettings that = (CustomElandInternalTextEmbeddingServiceSettings) o;
        return super.equals(that)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(similarityMeasure, that.similarityMeasure)
            && Objects.equals(elementType, that.elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimensions, similarityMeasure, elementType);
    }

}
