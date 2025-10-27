/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.common.MapPathExtractor;
import org.elasticsearch.xpack.inference.services.custom.CustomServiceEmbeddingType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.JSON_PARSER;

public class DenseEmbeddingResponseParser extends BaseCustomResponseParser {
    // This name is a holdover from before this class was renamed
    public static final String NAME = "text_embedding_response_parser";
    public static final String TEXT_EMBEDDING_PARSER_EMBEDDINGS = "text_embeddings";
    public static final String EMBEDDING_TYPE = "embedding_type";

    private static final TransportVersion ML_INFERENCE_CUSTOM_SERVICE_EMBEDDING_TYPE = TransportVersion.fromName(
        "ml_inference_custom_service_embedding_type"
    );

    public static DenseEmbeddingResponseParser fromMap(
        Map<String, Object> responseParserMap,
        String scope,
        ValidationException validationException
    ) {
        var jsonParserScope = String.join(".", scope, JSON_PARSER);
        var path = extractRequiredString(
            responseParserMap,
            TEXT_EMBEDDING_PARSER_EMBEDDINGS,
            String.join(".", scope, JSON_PARSER),
            validationException
        );

        var embeddingType = Objects.requireNonNullElse(
            extractOptionalEnum(
                responseParserMap,
                EMBEDDING_TYPE,
                jsonParserScope,
                CustomServiceEmbeddingType::fromString,
                EnumSet.allOf(CustomServiceEmbeddingType.class),
                validationException
            ),
            CustomServiceEmbeddingType.FLOAT
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new DenseEmbeddingResponseParser(path, embeddingType);
    }

    private final String textEmbeddingsPath;
    private final CustomServiceEmbeddingType embeddingType;

    public DenseEmbeddingResponseParser(String textEmbeddingsPath, CustomServiceEmbeddingType embeddingType) {
        this.textEmbeddingsPath = Objects.requireNonNull(textEmbeddingsPath);
        this.embeddingType = Objects.requireNonNull(embeddingType);
    }

    public DenseEmbeddingResponseParser(StreamInput in) throws IOException {
        this.textEmbeddingsPath = in.readString();
        if (in.getTransportVersion().supports(ML_INFERENCE_CUSTOM_SERVICE_EMBEDDING_TYPE)) {
            this.embeddingType = in.readEnum(CustomServiceEmbeddingType.class);
        } else {
            this.embeddingType = CustomServiceEmbeddingType.FLOAT;
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(textEmbeddingsPath);

        if (out.getTransportVersion().supports(ML_INFERENCE_CUSTOM_SERVICE_EMBEDDING_TYPE)) {
            out.writeEnum(embeddingType);
        }
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(JSON_PARSER);
        {
            builder.field(TEXT_EMBEDDING_PARSER_EMBEDDINGS, textEmbeddingsPath);
            builder.field(EMBEDDING_TYPE, embeddingType.toString());
        }
        builder.endObject();
        return builder;
    }

    // Default for testing
    String getTextEmbeddingsPath() {
        return textEmbeddingsPath;
    }

    @Override
    public CustomServiceEmbeddingType getEmbeddingType() {
        return embeddingType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DenseEmbeddingResponseParser that = (DenseEmbeddingResponseParser) o;
        return Objects.equals(textEmbeddingsPath, that.textEmbeddingsPath) && Objects.equals(embeddingType, that.embeddingType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(textEmbeddingsPath, embeddingType);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected InferenceServiceResults transform(Map<String, Object> map) {
        var extractedResult = MapPathExtractor.extract(map, textEmbeddingsPath);
        var mapResultsList = validateList(extractedResult.extractedObject(), extractedResult.getArrayFieldName(0));

        var embeddingConverter = createEmbeddingConverter(embeddingType);

        for (int i = 0; i < mapResultsList.size(); i++) {
            try {
                var entry = mapResultsList.get(i);
                embeddingConverter.toEmbedding(entry, extractedResult.getArrayFieldName(1));
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    Strings.format("Failed to parse text embedding entry [%d], error: %s", i, e.getMessage()),
                    e
                );
            }
        }

        return embeddingConverter.getResults();
    }

    private static EmbeddingConverter createEmbeddingConverter(CustomServiceEmbeddingType embeddingType) {
        return switch (embeddingType) {
            case FLOAT -> new FloatEmbeddings();
            case BYTE -> new ByteEmbeddings();
            case BINARY, BIT -> new BitEmbeddings();
        };
    }

    private interface EmbeddingConverter {
        void toEmbedding(Object entry, String fieldName);

        InferenceServiceResults getResults();
    }

    private static class FloatEmbeddings implements EmbeddingConverter {

        private final List<DenseEmbeddingFloatResults.Embedding> embeddings;

        FloatEmbeddings() {
            this.embeddings = new ArrayList<>();
        }

        public void toEmbedding(Object entry, String fieldName) {
            var embeddingsAsListFloats = convertToListOfFloats(entry, fieldName);
            embeddings.add(DenseEmbeddingFloatResults.Embedding.of(embeddingsAsListFloats));
        }

        public DenseEmbeddingFloatResults getResults() {
            return new DenseEmbeddingFloatResults(embeddings);
        }
    }

    private static class ByteEmbeddings implements EmbeddingConverter {

        private final List<DenseEmbeddingByteResults.Embedding> embeddings;

        ByteEmbeddings() {
            this.embeddings = new ArrayList<>();
        }

        public void toEmbedding(Object entry, String fieldName) {
            var convertedEmbeddings = convertToListOfBytes(entry, fieldName);
            this.embeddings.add(DenseEmbeddingByteResults.Embedding.of(convertedEmbeddings));
        }

        public DenseEmbeddingByteResults getResults() {
            return new DenseEmbeddingByteResults(embeddings);
        }
    }

    private static class BitEmbeddings implements EmbeddingConverter {

        private final List<DenseEmbeddingByteResults.Embedding> embeddings;

        BitEmbeddings() {
            this.embeddings = new ArrayList<>();
        }

        public void toEmbedding(Object entry, String fieldName) {
            var convertedEmbeddings = convertToListOfBits(entry, fieldName);
            this.embeddings.add(DenseEmbeddingByteResults.Embedding.of(convertedEmbeddings));
        }

        public DenseEmbeddingBitResults getResults() {
            return new DenseEmbeddingBitResults(embeddings);
        }
    }
}
