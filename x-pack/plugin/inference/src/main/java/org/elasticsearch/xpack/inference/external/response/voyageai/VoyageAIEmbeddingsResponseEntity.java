/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference.external.response.voyageai;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.InferenceByteEmbedding;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.request.voyageai.VoyageAIEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingType.toLowerCase;

public class VoyageAIEmbeddingsResponseEntity {
    private static final String VALID_EMBEDDING_TYPES_STRING = supportedEmbeddingTypes();

    private static String supportedEmbeddingTypes() {
        String[] validTypes = new String[] {
            toLowerCase(VoyageAIEmbeddingType.FLOAT),
            toLowerCase(VoyageAIEmbeddingType.INT8),
            toLowerCase(VoyageAIEmbeddingType.BIT) };
        Arrays.sort(validTypes);
        return String.join(", ", validTypes);
    }

    record EmbeddingInt8Result(List<EmbeddingInt8ResultEntry> entries, String model, String object, @Nullable Usage usage) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<EmbeddingInt8Result, Void> PARSER = new ConstructingObjectParser<>(
            EmbeddingInt8Result.class.getSimpleName(),
            args -> new EmbeddingInt8Result((List<EmbeddingInt8ResultEntry>) args[0], (String) args[1], (String) args[2], (Usage) args[3])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), EmbeddingInt8ResultEntry.PARSER::apply, new ParseField("data"));
            PARSER.declareString(constructorArg(), new ParseField("model"));
            PARSER.declareString(constructorArg(), new ParseField("object"));
            PARSER.declareObject(optionalConstructorArg(), Usage.PARSER::apply, new ParseField("usage"));
        }
    }

    record EmbeddingInt8ResultEntry(String object, Integer index, List<Integer> embedding) {

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<EmbeddingInt8ResultEntry, Void> PARSER = new ConstructingObjectParser<>(
            EmbeddingInt8ResultEntry.class.getSimpleName(),
            args -> new EmbeddingInt8ResultEntry((String) args[0], (Integer) args[1], (List<Integer>) args[2])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("object"));
            PARSER.declareInt(constructorArg(), new ParseField("index"));
            PARSER.declareIntArray(constructorArg(), new ParseField("embedding"));
        }

        private static void checkByteBounds(Integer value) {
            if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
            }
        }

        public InferenceByteEmbedding toInferenceByteEmbedding() {
            embedding.forEach(EmbeddingInt8ResultEntry::checkByteBounds);
            return InferenceByteEmbedding.of(embedding.stream().map(Integer::byteValue).toList());
        }
    }

    record EmbeddingFloatResult(List<EmbeddingFloatResultEntry> entries, String model, String object, @Nullable Usage usage) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<EmbeddingFloatResult, Void> PARSER = new ConstructingObjectParser<>(
            EmbeddingFloatResult.class.getSimpleName(),
            args -> new EmbeddingFloatResult((List<EmbeddingFloatResultEntry>) args[0], (String) args[1], (String) args[2], (Usage) args[3])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), EmbeddingFloatResultEntry.PARSER::apply, new ParseField("data"));
            PARSER.declareString(constructorArg(), new ParseField("model"));
            PARSER.declareString(constructorArg(), new ParseField("object"));
            PARSER.declareObject(optionalConstructorArg(), Usage.PARSER::apply, new ParseField("usage"));
        }
    }

    record EmbeddingFloatResultEntry(String object, Integer index, List<Float> embedding) {

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<EmbeddingFloatResultEntry, Void> PARSER = new ConstructingObjectParser<>(
            EmbeddingFloatResultEntry.class.getSimpleName(),
            args -> new EmbeddingFloatResultEntry((String) args[0], (Integer) args[1], (List<Float>) args[2])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("object"));
            PARSER.declareInt(constructorArg(), new ParseField("index"));
            PARSER.declareFloatArray(constructorArg(), new ParseField("embedding"));
        }

        public InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding toInferenceFloatEmbedding() {
            return InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding.of(embedding);
        }
    }

    record Usage(Integer totalTokens) {

        public static final ConstructingObjectParser<Usage, Void> PARSER = new ConstructingObjectParser<>(
            Usage.class.getSimpleName(),
            args -> new Usage((Integer) args[0])
        );

        static {
            PARSER.declareInt(constructorArg(), new ParseField("total_tokens"));
        }
    }

    /**
     * Parses the VoyageAI json response.
     * For a request like:
     *
     * <pre>
     *     <code>
     *        {
     *          "input": [
     *            "Sample text 1",
     *            "Sample text 2"
     *          ],
     *          "model": "voyage-3-large"
     *        }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     * <code>
     * {
     *  "object": "list",
     *  "data": [
     *      {
     *          "object": "embedding",
     *          "embedding": [
     *              -0.009327292,
     *              -0.0028842222,
     *          ],
     *          "index": 0
     *      },
     *      {
     *          "object": "embedding",
     *          "embedding": [ ... ],
     *          "index": 1
     *      }
     *  ],
     *  "model": "voyage-3-large",
     *  "usage": {
     *      "total_tokens": 10
     *  }
     * }
     * </code>
     * </pre>
     */
    public static InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        VoyageAIEmbeddingType embeddingType = ((VoyageAIEmbeddingsRequest) request).getServiceSettings().getEmbeddingType();

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            if (embeddingType == null || embeddingType == VoyageAIEmbeddingType.FLOAT) {
                var embeddingResult = EmbeddingFloatResult.PARSER.apply(jsonParser, null);

                List<InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding> embeddingList = embeddingResult.entries.stream()
                    .map(EmbeddingFloatResultEntry::toInferenceFloatEmbedding)
                    .toList();
                return new InferenceTextEmbeddingFloatResults(embeddingList);
            } else if (embeddingType == VoyageAIEmbeddingType.INT8) {
                var embeddingResult = EmbeddingInt8Result.PARSER.apply(jsonParser, null);
                List<InferenceByteEmbedding> embeddingList = embeddingResult.entries.stream()
                    .map(EmbeddingInt8ResultEntry::toInferenceByteEmbedding)
                    .toList();
                return new InferenceTextEmbeddingByteResults(embeddingList);
            } else if (embeddingType == VoyageAIEmbeddingType.BIT || embeddingType == VoyageAIEmbeddingType.BINARY) {
                var embeddingResult = EmbeddingInt8Result.PARSER.apply(jsonParser, null);
                List<InferenceByteEmbedding> embeddingList = embeddingResult.entries.stream()
                    .map(EmbeddingInt8ResultEntry::toInferenceByteEmbedding)
                    .toList();
                return new InferenceTextEmbeddingBitResults(embeddingList);
            } else {
                throw new IllegalArgumentException(
                    "Illegal embedding_type value: " + embeddingType + ". Supported types are: " + VALID_EMBEDDING_TYPES_STRING
                );
            }
        }
    }

    private VoyageAIEmbeddingsResponseEntity() {}
}
