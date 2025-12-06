/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.contextual.VoyageAIContextualEmbeddingType;
import org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIContextualizedEmbeddingsRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.inference.services.voyageai.embeddings.contextual.VoyageAIContextualEmbeddingType.toLowerCase;

/**
 * Response entity for VoyageAI contextualized embeddings API.
 *
 * The key difference from regular embeddings is that the response contains nested embeddings
 * for each document (data[].embeddings instead of data[].embedding).
 */
public class VoyageAIContextualizedEmbeddingsResponseEntity {
    private static final String VALID_EMBEDDING_TYPES_STRING = supportedEmbeddingTypes();

    private static String supportedEmbeddingTypes() {
        String[] validTypes = new String[] {
            toLowerCase(VoyageAIContextualEmbeddingType.FLOAT),
            toLowerCase(VoyageAIContextualEmbeddingType.INT8),
            toLowerCase(VoyageAIContextualEmbeddingType.BIT) };
        Arrays.sort(validTypes);
        return String.join(", ", validTypes);
    }

    // Top-level result that contains an array of contextualized embedding batches
    record EmbeddingInt8Result(List<EmbeddingInt8ContextBatch> batches) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<EmbeddingInt8Result, Void> PARSER = new ConstructingObjectParser<>(
            EmbeddingInt8Result.class.getSimpleName(),
            true,
            args -> new EmbeddingInt8Result((List<EmbeddingInt8ContextBatch>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), EmbeddingInt8ContextBatch.PARSER::apply, new ParseField("data"));
        }
    }

    // Each batch contains multiple embeddings for a contextualized input
    record EmbeddingInt8ContextBatch(List<EmbeddingInt8SingleEntry> embeddings) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<EmbeddingInt8ContextBatch, Void> PARSER = new ConstructingObjectParser<>(
            EmbeddingInt8ContextBatch.class.getSimpleName(),
            true,
            args -> new EmbeddingInt8ContextBatch((List<EmbeddingInt8SingleEntry>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), EmbeddingInt8SingleEntry.PARSER::apply, new ParseField("data"));
        }

        public List<DenseEmbeddingByteResults.Embedding> toInferenceByteEmbeddings() {
            return embeddings.stream()
                .map(EmbeddingInt8SingleEntry::toInferenceByteEmbedding)
                .toList();
        }
    }

    // Individual embedding entry (similar to text embeddings)
    record EmbeddingInt8SingleEntry(Integer index, List<Integer> embedding) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<EmbeddingInt8SingleEntry, Void> PARSER = new ConstructingObjectParser<>(
            EmbeddingInt8SingleEntry.class.getSimpleName(),
            true,
            args -> new EmbeddingInt8SingleEntry((Integer) args[0], (List<Integer>) args[1])
        );

        static {
            PARSER.declareInt(constructorArg(), new ParseField("index"));
            PARSER.declareIntArray(constructorArg(), new ParseField("embedding"));
        }

        private static void checkByteBounds(Integer value) {
            if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
            }
        }

        public DenseEmbeddingByteResults.Embedding toInferenceByteEmbedding() {
            embedding.forEach(EmbeddingInt8SingleEntry::checkByteBounds);
            byte[] embeddingArray = new byte[embedding.size()];
            for (int i = 0; i < embedding.size(); i++) {
                embeddingArray[i] = embedding.get(i).byteValue();
            }
            return new DenseEmbeddingByteResults.Embedding(embeddingArray, null, 0);
        }
    }

    // Top-level result that contains an array of contextualized embedding batches
    record EmbeddingFloatResult(List<EmbeddingFloatContextBatch> batches) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<EmbeddingFloatResult, Void> PARSER = new ConstructingObjectParser<>(
            EmbeddingFloatResult.class.getSimpleName(),
            true,
            args -> new EmbeddingFloatResult((List<EmbeddingFloatContextBatch>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), EmbeddingFloatContextBatch.PARSER::apply, new ParseField("data"));
        }
    }

    // Each batch contains multiple embeddings for a contextualized input
    record EmbeddingFloatContextBatch(List<EmbeddingFloatSingleEntry> embeddings) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<EmbeddingFloatContextBatch, Void> PARSER = new ConstructingObjectParser<>(
            EmbeddingFloatContextBatch.class.getSimpleName(),
            true,
            args -> new EmbeddingFloatContextBatch((List<EmbeddingFloatSingleEntry>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), EmbeddingFloatSingleEntry.PARSER::apply, new ParseField("data"));
        }

        public List<DenseEmbeddingFloatResults.Embedding> toInferenceFloatEmbeddings() {
            return embeddings.stream()
                .map(EmbeddingFloatSingleEntry::toInferenceFloatEmbedding)
                .toList();
        }
    }

    // Individual embedding entry (similar to text embeddings)
    record EmbeddingFloatSingleEntry(Integer index, List<Float> embedding) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<EmbeddingFloatSingleEntry, Void> PARSER = new ConstructingObjectParser<>(
            EmbeddingFloatSingleEntry.class.getSimpleName(),
            true,
            args -> new EmbeddingFloatSingleEntry((Integer) args[0], (List<Float>) args[1])
        );

        static {
            PARSER.declareInt(constructorArg(), new ParseField("index"));
            PARSER.declareFloatArray(constructorArg(), new ParseField("embedding"));
        }

        public DenseEmbeddingFloatResults.Embedding toInferenceFloatEmbedding() {
            float[] embeddingArray = new float[embedding.size()];
            for (int i = 0; i < embedding.size(); i++) {
                embeddingArray[i] = embedding.get(i);
            }
            return new DenseEmbeddingFloatResults.Embedding(embeddingArray, 0);
        }
    }

    /**
     * Parses the VoyageAI contextualized embeddings json response.
     * The response format differs from regular embeddings in that it contains nested embeddings:
     *
     * <pre>
     * <code>
     * {
     *  "object": "list",
     *  "data": [
     *      {
     *          "embeddings": [
     *              [-0.009327292, -0.0028842222, ...],
     *              [0.009327292, 0.0028842222, ...]
     *          ],
     *          "index": 0
     *      }
     *  ],
     *  "model": "voyage-context-3",
     *  "usage": {
     *      "total_tokens": 25
     *  }
     * }
     * </code>
     * </pre>
     */
    public static InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        VoyageAIContextualEmbeddingType embeddingType = ((VoyageAIContextualizedEmbeddingsRequest) request).getServiceSettings().getEmbeddingType();

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            if (embeddingType == null || embeddingType == VoyageAIContextualEmbeddingType.FLOAT) {
                var embeddingResult = EmbeddingFloatResult.PARSER.apply(jsonParser, null);

                // Flatten the nested embeddings into a single list
                List<DenseEmbeddingFloatResults.Embedding> embeddingList = new ArrayList<>();
                for (var batch : embeddingResult.batches) {
                    embeddingList.addAll(batch.toInferenceFloatEmbeddings());
                }
                return new DenseEmbeddingFloatResults(embeddingList);
            } else if (embeddingType == VoyageAIContextualEmbeddingType.INT8) {
                var embeddingResult = EmbeddingInt8Result.PARSER.apply(jsonParser, null);

                // Flatten the nested embeddings into a single list
                List<DenseEmbeddingByteResults.Embedding> embeddingList = new ArrayList<>();
                for (var batch : embeddingResult.batches) {
                    embeddingList.addAll(batch.toInferenceByteEmbeddings());
                }
                return new DenseEmbeddingByteResults(embeddingList);
            } else if (embeddingType == VoyageAIContextualEmbeddingType.BIT || embeddingType == VoyageAIContextualEmbeddingType.BINARY) {
                var embeddingResult = EmbeddingInt8Result.PARSER.apply(jsonParser, null);

                // Flatten the nested embeddings into a single list
                List<DenseEmbeddingByteResults.Embedding> embeddingList = new ArrayList<>();
                for (var batch : embeddingResult.batches) {
                    embeddingList.addAll(batch.toInferenceByteEmbeddings());
                }
                return new DenseEmbeddingBitResults(embeddingList);
            } else {
                throw new IllegalArgumentException(
                    "Illegal embedding_type value: " + embeddingType + ". Supported types are: " + VALID_EMBEDDING_TYPES_STRING
                );
            }
        }
    }

    private VoyageAIContextualizedEmbeddingsResponseEntity() {}
}
