/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.response;

import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.XContentUtils;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIEmbeddingsRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.consumeUntilObjectEnd;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType.toLowerCase;

public class JinaAIEmbeddingsResponseEntity {
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in JinaAI embeddings response";

    private static final Map<String, CheckedBiFunction<XContentParser, TaskType, InferenceServiceResults, IOException>> EMBEDDING_PARSERS =
        Map.of(
            toLowerCase(JinaAIEmbeddingType.FLOAT),
            JinaAIEmbeddingsResponseEntity::parseFloatDataObject,
            toLowerCase(JinaAIEmbeddingType.BIT),
            JinaAIEmbeddingsResponseEntity::parseBitDataObject,
            toLowerCase(JinaAIEmbeddingType.BINARY),
            JinaAIEmbeddingsResponseEntity::parseBitDataObject
        );
    private static final String VALID_EMBEDDING_TYPES_STRING = supportedEmbeddingTypes();

    private static String supportedEmbeddingTypes() {
        var validTypes = EMBEDDING_PARSERS.keySet().toArray(String[]::new);
        Arrays.sort(validTypes);
        return String.join(", ", validTypes);
    }

    /**
     * Parses the JinaAI json response.
     * For a request like:
     *
     * <pre>
     *     <code>
     *        {
     *            "inputs": ["hello this is my name", "I wish I was there!"]
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
     *  "model": "jina-embeddings-v3",
     *  "usage": {
     *      "prompt_tokens": 8,
     *      "total_tokens": 8
     *  }
     * }
     * </code>
     * </pre>
     */
    public static InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        // embeddings type is not specified anywhere in the response so grab it from the request
        JinaAIEmbeddingsRequest embeddingsRequest = (JinaAIEmbeddingsRequest) request;
        var embeddingType = embeddingsRequest.getEmbeddingType().toString();
        var embeddingValueParser = EMBEDDING_PARSERS.get(embeddingType);

        if (embeddingValueParser == null) {
            throw new IllegalStateException(
                Strings.format(
                    "Failed to find a supported embedding type for in the Jina AI embeddings response. Supported types are [%s]",
                    VALID_EMBEDDING_TYPES_STRING
                )
            );
        }

        var taskType = embeddingsRequest.getTaskType();
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "data", FAILED_TO_FIND_FIELD_TEMPLATE);

            return embeddingValueParser.apply(jsonParser, taskType);
        }
    }

    private static InferenceServiceResults parseFloatDataObject(XContentParser jsonParser, TaskType taskType) throws IOException {
        List<EmbeddingFloatResults.Embedding> embeddingList = parseList(
            jsonParser,
            JinaAIEmbeddingsResponseEntity::parseFloatEmbeddingObject
        );

        if (taskType == TaskType.TEXT_EMBEDDING) {
            return new DenseEmbeddingFloatResults(embeddingList);
        } else if (taskType == TaskType.EMBEDDING) {
            return new GenericDenseEmbeddingFloatResults(embeddingList);
        } else {
            throw new IllegalArgumentException("Invalid taskType: " + taskType);
        }
    }

    private static EmbeddingFloatResults.Embedding parseFloatEmbeddingObject(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        positionParserAtTokenAfterField(parser, "embedding", FAILED_TO_FIND_FIELD_TEMPLATE);

        var embeddingValuesList = parseList(parser, XContentUtils::parseFloat);
        // parse and discard the rest of the object
        consumeUntilObjectEnd(parser);

        return EmbeddingFloatResults.Embedding.of(embeddingValuesList);
    }

    private static InferenceServiceResults parseBitDataObject(XContentParser jsonParser, TaskType taskType) throws IOException {
        List<DenseEmbeddingByteResults.Embedding> embeddingList = parseList(
            jsonParser,
            JinaAIEmbeddingsResponseEntity::parseBitEmbeddingObject
        );

        if (taskType == TaskType.TEXT_EMBEDDING) {
            return new DenseEmbeddingBitResults(embeddingList);
        } else if (taskType == TaskType.EMBEDDING) {
            return new GenericDenseEmbeddingBitResults(embeddingList);
        } else {
            throw new IllegalArgumentException("Invalid taskType: " + taskType);
        }
    }

    private static EmbeddingByteResults.Embedding parseBitEmbeddingObject(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        positionParserAtTokenAfterField(parser, "embedding", FAILED_TO_FIND_FIELD_TEMPLATE);

        var embeddingList = parseList(parser, JinaAIEmbeddingsResponseEntity::parseEmbeddingInt8Entry);
        // parse and discard the rest of the object
        consumeUntilObjectEnd(parser);

        return EmbeddingByteResults.Embedding.of(embeddingList);
    }

    private static Byte parseEmbeddingInt8Entry(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
        var parsedByte = parser.shortValue();
        checkByteBounds(parsedByte);

        return (byte) parsedByte;
    }

    private static void checkByteBounds(short value) {
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
        }
    }

    private JinaAIEmbeddingsResponseEntity() {}
}
