/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

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

public class CustomElandInternalServiceSettings extends ElasticsearchInternalServiceSettings {

    public static final String NAME = "custom_eland_model_internal_service_settings";

    private static final int FAILED_INT_PARSE_VALUE = -1;

    private final Integer dimensions;
    private final SimilarityMeasure similarityMeasure;
    private final DenseVectorFieldMapper.ElementType elementType;

    public CustomElandInternalServiceSettings(int numAllocations, int numThreads, String modelId) {
        this(numAllocations, numThreads, modelId, null, null, null);
    }

    public CustomElandInternalServiceSettings(
        int numAllocations,
        int numThreads,
        String modelId,
        Integer dimensions,
        SimilarityMeasure similarityMeasure,
        DenseVectorFieldMapper.ElementType elementType
    ) {
        super(numAllocations, numThreads, modelId);
        this.dimensions = dimensions;
        this.similarityMeasure = similarityMeasure;
        this.elementType = elementType;
    }

    public CustomElandInternalServiceSettings(StreamInput in) throws IOException {
        super(in.readVInt(), in.readVInt(), in.readString());
        if (in.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_ELAND_SETTINGS_ADDED)) {
            dimensions = in.readOptionalVInt();
            similarityMeasure = in.readOptionalEnum(SimilarityMeasure.class);
            elementType = in.readOptionalEnum(DenseVectorFieldMapper.ElementType.class);
        } else {
            dimensions = null;
            similarityMeasure = null;
            elementType = null;
        }
    }

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
    public static Builder fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return switch (context) {
            case REQUEST -> fromRequestMap(map);
            case PERSISTENT -> fromPersistentMap(map);
        };
    }

    private static Builder fromRequestMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        var commonFields = commonFieldsFromMap(map, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        var defaultedCommonFields = new CommonFields(
            commonFields.numAllocations,
            commonFields.numThreads,
            commonFields.modelId,
            Objects.requireNonNullElse(commonFields.similarityMeasure, SimilarityMeasure.COSINE),
            Objects.requireNonNullElse(commonFields.elementType, DenseVectorFieldMapper.ElementType.FLOAT)
        );

        return new ElandBuilder(defaultedCommonFields);
    }

    private static Builder fromPersistentMap(Map<String, Object> map) {
        var commonFields = commonFieldsFromMap(map);
        Integer dims = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, new ValidationException());

        return new ElandBuilder(commonFields, dims);
    }

    private record CommonFields(
        int numAllocations,
        int numThreads,
        String modelId,
        SimilarityMeasure similarityMeasure,
        DenseVectorFieldMapper.ElementType elementType
    ) {}

    private static CommonFields commonFieldsFromMap(Map<String, Object> map) {
        return commonFieldsFromMap(map, new ValidationException());
    }

    private static CommonFields commonFieldsFromMap(Map<String, Object> map, ValidationException validationException) {
        Integer numAllocations = extractOptionalPositiveInteger(
            map,
            NUM_ALLOCATIONS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        Integer numThreads = extractOptionalPositiveInteger(map, NUM_THREADS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        validateParameters(numAllocations, validationException, numThreads);

        String modelId = ServiceUtils.extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
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
            // if an error occurred while parsing, we'll set these to an invalid value so we don't accidentally get a
            // null pointer when doing unboxing
            Objects.requireNonNullElse(numAllocations, FAILED_INT_PARSE_VALUE),
            Objects.requireNonNullElse(numThreads, FAILED_INT_PARSE_VALUE),
            modelId,
            similarity,
            elementType
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        var superBuilder = toXContentFragment(builder, params);

        if (dimensions != null) {
            superBuilder.field(DIMENSIONS, dimensions);
        }

        if (similarityMeasure != null) {
            superBuilder.field(SIMILARITY, similarityMeasure);
        }

        if (elementType != null) {
            superBuilder.field(ELEMENT_TYPE, elementType);
        }

        superBuilder.endObject();
        return superBuilder;
    }

    @Override
    public boolean isFragment() {
        return super.isFragment();
    }

    @Override
    public String getWriteableName() {
        return CustomElandInternalServiceSettings.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_13_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (out.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_ELAND_SETTINGS_ADDED)) {
            out.writeOptionalVInt(dimensions);
            out.writeOptionalEnum(similarityMeasure);
            out.writeOptionalEnum(elementType);
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

    private static class ElandBuilder extends Builder {
        private final Integer dimensions;
        private final CommonFields commonFields;

        private ElandBuilder(CommonFields commonFields) {
            this(commonFields, null);
        }

        private ElandBuilder(CommonFields commonFields, @Nullable Integer dimensions) {
            super(commonFields.numAllocations, commonFields.numThreads, commonFields.modelId);

            this.dimensions = dimensions;
            this.commonFields = commonFields;
        }

        @Override
        public CustomElandInternalServiceSettings build() {
            return new CustomElandInternalServiceSettings(
                getNumAllocations(),
                getNumThreads(),
                getModelId(),
                dimensions,
                commonFields.similarityMeasure,
                commonFields.elementType
            );
        }
    }
}
