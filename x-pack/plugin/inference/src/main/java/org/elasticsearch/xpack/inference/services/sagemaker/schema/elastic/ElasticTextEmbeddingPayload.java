/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredServiceSchema;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;

/**
 * TextEmbedding needs to differentiate between Bit, Byte, and Float types. Users must specify the
 * {@link org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType} in the Service Settings,
 * and Elastic will use that to parse the request/response. {@link SimilarityMeasure} and Dimensions are also needed, though Dimensions can
 * be guessed and set during the validation call.
 * At the very least, Service Settings must look like:
 * {
 *     "element_type": "bit|byte|float",
 *     "similarity": "cosine|dot_product|l2_norm"
 * }
 */
public class ElasticTextEmbeddingPayload implements ElasticPayload {
    private static final EnumSet<TaskType> SUPPORTED_TASKS = EnumSet.of(TaskType.TEXT_EMBEDDING);
    private static final ParseField EMBEDDING = new ParseField("embedding");

    @Override
    public EnumSet<TaskType> supportedTasks() {
        return SUPPORTED_TASKS;
    }

    @Override
    public SageMakerStoredServiceSchema apiServiceSettings(Map<String, Object> serviceSettings, ValidationException validationException) {
        return ApiServiceSettings.fromMap(serviceSettings, validationException);
    }

    @Override
    public SdkBytes requestBytes(SageMakerModel model, SageMakerInferenceRequest request) throws Exception {
        if (model.apiServiceSettings() instanceof ApiServiceSettings) {
            return ElasticPayload.super.requestBytes(model, request);
        } else {
            throw createUnsupportedSchemaException(model);
        }
    }

    @Override
    public Stream<NamedWriteableRegistry.Entry> namedWriteables() {
        return Stream.concat(
            ElasticPayload.super.namedWriteables(),
            Stream.of(
                new NamedWriteableRegistry.Entry(SageMakerStoredServiceSchema.class, ApiServiceSettings.NAME, ApiServiceSettings::new)
            )
        );
    }

    @Override
    public TextEmbeddingResults<?> responseBody(SageMakerModel model, InvokeEndpointResponse response) throws Exception {
        try (var p = jsonXContent.createParser(XContentParserConfiguration.EMPTY, response.body().asInputStream())) {
            return switch (model.apiServiceSettings().elementType()) {
                case BIT -> TextEmbeddingBinary.PARSER.apply(p, null);
                case BYTE -> TextEmbeddingBytes.PARSER.apply(p, null);
                case FLOAT -> TextEmbeddingFloat.PARSER.apply(p, null);
            };
        }
    }

    /**
     * Reads binary format (it says bytes, but the lengths are different)
     * {
     *     "text_embedding_bits": [
     *         {
     *             "embedding": [
     *                 23
     *             ]
     *         },
     *         {
     *             "embedding": [
     *                 -23
     *             ]
     *         }
     *     ]
     * }
     */
    private static class TextEmbeddingBinary {
        private static final ParseField TEXT_EMBEDDING_BITS = new ParseField(TextEmbeddingBitResults.TEXT_EMBEDDING_BITS);
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<TextEmbeddingBitResults, Void> PARSER = new ConstructingObjectParser<>(
            TextEmbeddingBitResults.class.getSimpleName(),
            IGNORE_UNKNOWN_FIELDS,
            args -> new TextEmbeddingBitResults((List<TextEmbeddingByteResults.Embedding>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), TextEmbeddingBytes.BYTE_PARSER::apply, TEXT_EMBEDDING_BITS);
        }
    }

    /**
     * Reads byte format from
     * {
     *     "text_embedding_bytes": [
     *         {
     *             "embedding": [
     *                 23
     *             ]
     *         },
     *         {
     *             "embedding": [
     *                 -23
     *             ]
     *         }
     *     ]
     * }
     */
    private static class TextEmbeddingBytes {
        private static final ParseField TEXT_EMBEDDING_BYTES = new ParseField("text_embedding_bytes");
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<TextEmbeddingByteResults, Void> PARSER = new ConstructingObjectParser<>(
            TextEmbeddingByteResults.class.getSimpleName(),
            IGNORE_UNKNOWN_FIELDS,
            args -> new TextEmbeddingByteResults((List<TextEmbeddingByteResults.Embedding>) args[0])
        );

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<TextEmbeddingByteResults.Embedding, Void> BYTE_PARSER =
            new ConstructingObjectParser<>(
                TextEmbeddingByteResults.Embedding.class.getSimpleName(),
                IGNORE_UNKNOWN_FIELDS,
                args -> TextEmbeddingByteResults.Embedding.of((List<Byte>) args[0])
            );

        static {
            BYTE_PARSER.declareObjectArray(constructorArg(), (p, c) -> {
                var byteVal = p.shortValue();
                if (byteVal < Byte.MIN_VALUE || byteVal > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + byteVal + "] is out of range for a byte");
                }
                return (byte) byteVal;
            }, EMBEDDING);
            PARSER.declareObjectArray(constructorArg(), BYTE_PARSER::apply, TEXT_EMBEDDING_BYTES);
        }
    }

    /**
     * Reads float format from
     * {
     *     "text_embedding": [
     *         {
     *             "embedding": [
     *                 0.1
     *             ]
     *         },
     *         {
     *             "embedding": [
     *                 0.2
     *             ]
     *         }
     *     ]
     * }
     */
    private static class TextEmbeddingFloat {
        private static final ParseField TEXT_EMBEDDING_FLOAT = new ParseField("text_embedding");
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<TextEmbeddingFloatResults, Void> PARSER = new ConstructingObjectParser<>(
            TextEmbeddingByteResults.class.getSimpleName(),
            IGNORE_UNKNOWN_FIELDS,
            args -> new TextEmbeddingFloatResults((List<TextEmbeddingFloatResults.Embedding>) args[0])
        );

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<TextEmbeddingFloatResults.Embedding, Void> FLOAT_PARSER =
            new ConstructingObjectParser<>(
                TextEmbeddingFloatResults.Embedding.class.getSimpleName(),
                IGNORE_UNKNOWN_FIELDS,
                args -> TextEmbeddingFloatResults.Embedding.of((List<Float>) args[0])
            );

        static {
            FLOAT_PARSER.declareFloatArray(constructorArg(), EMBEDDING);
            PARSER.declareObjectArray(constructorArg(), FLOAT_PARSER::apply, TEXT_EMBEDDING_FLOAT);
        }
    }

    /**
     * Element Type is required. It is used to disambiguate between binary embeddings and byte embeddings.
     */
    record ApiServiceSettings(
        @Nullable Integer dimensions,
        Boolean dimensionsSetByUser,
        @Nullable SimilarityMeasure similarity,
        DenseVectorFieldMapper.ElementType elementType
    ) implements SageMakerStoredServiceSchema {

        private static final String NAME = "sagemaker_elastic_text_embeddings_service_settings";
        private static final String DIMENSIONS_FIELD = "dimensions";
        private static final String DIMENSIONS_SET_BY_USER_FIELD = "dimensions_set_by_user";
        private static final String SIMILARITY_FIELD = "similarity";
        private static final String ELEMENT_TYPE_FIELD = "element_type";

        ApiServiceSettings(StreamInput in) throws IOException {
            this(
                in.readOptionalInt(),
                in.readBoolean(),
                in.readOptionalEnum(SimilarityMeasure.class),
                in.readEnum(DenseVectorFieldMapper.ElementType.class)
            );
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ML_INFERENCE_SAGEMAKER_ELASTIC;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalInt(dimensions);
            out.writeBoolean(dimensionsSetByUser);
            out.writeOptionalEnum(similarity);
            out.writeEnum(elementType);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (dimensions != null) {
                builder.field(DIMENSIONS_FIELD, dimensions);
            }
            builder.field(DIMENSIONS_SET_BY_USER_FIELD, dimensionsSetByUser);
            if (similarity != null) {
                builder.field(SIMILARITY_FIELD, similarity);
            }
            builder.field(ELEMENT_TYPE_FIELD, elementType);
            return builder;
        }

        @Override
        public ApiServiceSettings updateModelWithEmbeddingDetails(Integer dimensions) {
            return new ApiServiceSettings(dimensions, false, similarity, elementType);
        }

        static ApiServiceSettings fromMap(Map<String, Object> serviceSettings, ValidationException validationException) {
            var dimensions = extractOptionalPositiveInteger(
                serviceSettings,
                DIMENSIONS_FIELD,
                ModelConfigurations.SERVICE_SETTINGS,
                validationException
            );
            var dimensionsSetByUser = extractOptionalBoolean(serviceSettings, DIMENSIONS_SET_BY_USER_FIELD, validationException);
            var similarity = extractSimilarity(serviceSettings, ModelConfigurations.SERVICE_SETTINGS, validationException);
            var elementType = extractRequiredEnum(
                serviceSettings,
                ELEMENT_TYPE_FIELD,
                ModelConfigurations.SERVICE_SETTINGS,
                DenseVectorFieldMapper.ElementType::fromString,
                EnumSet.allOf(DenseVectorFieldMapper.ElementType.class),
                validationException
            );
            return new ApiServiceSettings(dimensions, dimensionsSetByUser != null && dimensionsSetByUser, similarity, elementType);
        }
    }
}
