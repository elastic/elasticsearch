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
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.InternalServiceSettings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.ML_INFERENCE_L2_NORM_SIMILARITY_ADDED;
import static org.elasticsearch.TransportVersions.ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;

public class MultilingualE5SmallInternalServiceSettings extends ElasticsearchInternalServiceSettings {

    public static final String NAME = "multilingual_e5_small_service_settings";

    static final int DIMENSIONS = 384;
    static final SimilarityMeasure DEFAULT_SIMILARITY = SimilarityMeasure.COSINE;

    private final SimilarityMeasure similarity;
    private final int dimensions;

    public MultilingualE5SmallInternalServiceSettings(
        int numAllocations,
        int numThreads,
        String modelId,
        SimilarityMeasure similarityMeasure,
        int dimensions
    ) {
        super(numAllocations, numThreads, modelId);

        this.similarity = similarityMeasure;
        this.dimensions = dimensions;
    }

    public MultilingualE5SmallInternalServiceSettings(StreamInput in) throws IOException {
        super(in.readVInt(), in.readVInt(), in.readString());

        if (in.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_L2_NORM_SIMILARITY_ADDED)) {
            similarity = in.readEnum(SimilarityMeasure.class);
            dimensions = in.readVInt();
        } else {
            similarity = DEFAULT_SIMILARITY;
            dimensions = DIMENSIONS;
        }
    }

    /**
     * Parse the MultilingualE5SmallServiceSettings from map that originated from persistent storage and validate the setting values.
     *
     * If required setting are missing or the values are invalid an
     * {@link ValidationException} is thrown.
     *
     * @param map Source map containing the config
     * @return The {@code MultilingualE5SmallServiceSettings} builder
     */
    public static MultilingualE5SmallInternalServiceSettings.Builder fromPersistentMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        var requestFields = extractRequestFields(map, validationException);

        Integer parsedDimensions = ServiceUtils.removeAsType(map, ServiceFields.DIMENSIONS, Integer.class);
        if (parsedDimensions == null) {
            parsedDimensions = DIMENSIONS;
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return createBuilder(requestFields, parsedDimensions);
    }

    private static MultilingualE5SmallInternalServiceSettings.Builder createBuilder(RequestFields requestFields, int dimensions) {
        var builder = new InternalServiceSettings.Builder() {
            @Override
            public MultilingualE5SmallInternalServiceSettings build() {
                return new MultilingualE5SmallInternalServiceSettings(
                    getNumAllocations(),
                    getNumThreads(),
                    getModelId(),
                    requestFields.similarityMeasure,
                    dimensions
                );
            }
        };
        builder.setNumAllocations(requestFields.numAllocations);
        builder.setNumThreads(requestFields.numThreads);
        builder.setModelId(requestFields.modelId);
        return builder;
    }

    private static RequestFields extractRequestFields(Map<String, Object> map, ValidationException validationException) {
        Integer numAllocations = ServiceUtils.removeAsType(map, NUM_ALLOCATIONS, Integer.class);
        Integer numThreads = ServiceUtils.removeAsType(map, NUM_THREADS, Integer.class);

        validateParameters(numAllocations, validationException, numThreads);

        String modelId = ServiceUtils.removeAsType(map, MODEL_ID, String.class);
        if (modelId != null) {
            if (ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_VALID_IDS.contains(modelId) == false) {
                validationException.addValidationError(
                    "unknown Multilingual-E5-Small model ID ["
                        + modelId
                        + "]. Valid IDs are "
                        + Arrays.toString(ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_VALID_IDS.toArray())
                );
            }
        }

        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        if (similarity == null) {
            similarity = DEFAULT_SIMILARITY;
        }

        return new RequestFields(numAllocations, numThreads, modelId, similarity);
    }

    private record RequestFields(
        @Nullable Integer numAllocations,
        @Nullable Integer numThreads,
        @Nullable String modelId,
        SimilarityMeasure similarityMeasure
    ) {}

    /**
     * Parse the MultilingualE5SmallServiceSettings from map and validate the setting values.
     *
     * If required setting are missing or the values are invalid an
     * {@link ValidationException} is thrown.
     *
     * @param map Source map containing the config
     * @return The {@code MultilingualE5SmallServiceSettings} builder
     */
    public static MultilingualE5SmallInternalServiceSettings.Builder fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        var requestFields = extractRequestFields(map, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return createBuilder(requestFields, DIMENSIONS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUM_ALLOCATIONS, getNumAllocations());
        builder.field(NUM_THREADS, getNumThreads());
        builder.field(MODEL_ID, getModelId());
        builder.field(ServiceFields.SIMILARITY, similarity);
        builder.field(ServiceFields.DIMENSIONS, dimensions);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isFragment() {
        return super.isFragment();
    }

    @Override
    public String getWriteableName() {
        return MultilingualE5SmallInternalServiceSettings.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (out.getTransportVersion().onOrAfter(ML_INFERENCE_L2_NORM_SIMILARITY_ADDED)) {
            out.writeEnum(similarity);
            out.writeVInt(dimensions);
        }
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultilingualE5SmallInternalServiceSettings that = (MultilingualE5SmallInternalServiceSettings) o;
        return this.getNumAllocations() == that.getNumAllocations()
            && this.getNumThreads() == that.getNumThreads()
            && Objects.equals(this.getModelId(), that.getModelId())
            && Objects.equals(this.similarity, that.similarity)
            && this.dimensions == that.dimensions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNumAllocations(), getNumThreads(), getModelId(), similarity, dimensions);
    }
}
