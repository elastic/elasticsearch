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
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.inference.InferenceServiceResults;
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
import org.elasticsearch.xpack.inference.external.response.XContentUtils;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingType;

import static org.elasticsearch.xpack.inference.external.response.XContentUtils.consumeUntilObjectEnd;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingType.toLowerCase;

public class VoyageAIEmbeddingsResponseEntity {
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in VoyageAI embeddings response";
    private static final Map<String, CheckedFunction<XContentParser, InferenceServiceResults, IOException>> EMBEDDING_PARSERS = Map.of(
        toLowerCase(VoyageAIEmbeddingType.FLOAT),
        VoyageAIEmbeddingsResponseEntity::parseFloatEmbeddingsArray,
        toLowerCase(VoyageAIEmbeddingType.INT8),
        VoyageAIEmbeddingsResponseEntity::parseByteEmbeddingsArray,
        toLowerCase(VoyageAIEmbeddingType.BINARY),
        VoyageAIEmbeddingsResponseEntity::parseBitEmbeddingsArray
    );

    private static final String VALID_EMBEDDING_TYPES_STRING = supportedEmbeddingTypes();

    private static String supportedEmbeddingTypes() {
        var validTypes = EMBEDDING_PARSERS.keySet().toArray(String[]::new);
        Arrays.sort(validTypes);
        return String.join(", ", validTypes);
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
        VoyageAIEmbeddingType embeddingType = ((VoyageAIEmbeddingsRequest)request).getServiceSettings().getEmbeddingType();

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "data", FAILED_TO_FIND_FIELD_TEMPLATE);

            if(embeddingType == null || embeddingType == VoyageAIEmbeddingType.FLOAT) {
                List<InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding> embeddingList = parseList(
                    jsonParser,
                    VoyageAIEmbeddingsResponseEntity::parseEmbeddingObjectFloat
                );

                return new InferenceTextEmbeddingFloatResults(embeddingList);
            } else if(embeddingType == VoyageAIEmbeddingType.INT8) {
                List<InferenceByteEmbedding> embeddingList = parseList(
                    jsonParser,
                    VoyageAIEmbeddingsResponseEntity::parseEmbeddingObjectByte
                );

                return new InferenceTextEmbeddingByteResults(embeddingList);
            } else {
                throw new IllegalArgumentException("Illegal output_dtype value: " + embeddingType);
            }
        }
    }

    private static InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding parseEmbeddingObjectFloat(XContentParser parser)
        throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        positionParserAtTokenAfterField(parser, "embedding", FAILED_TO_FIND_FIELD_TEMPLATE);

        List<Float> embeddingValuesList = parseList(parser, XContentUtils::parseFloat);
        // parse and discard the rest of the object
        consumeUntilObjectEnd(parser);

        return InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding.of(embeddingValuesList);
    }

    private static InferenceByteEmbedding parseEmbeddingObjectByte(XContentParser parser)
        throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        positionParserAtTokenAfterField(parser, "embedding", FAILED_TO_FIND_FIELD_TEMPLATE);

        List<Byte> embeddingValuesList = parseList(parser, VoyageAIEmbeddingsResponseEntity::parseEmbeddingInt8Entry);
        // parse and discard the rest of the object
        consumeUntilObjectEnd(parser);

        return InferenceByteEmbedding.of(embeddingValuesList);
    }

    private static InferenceServiceResults parseBitEmbeddingsArray(XContentParser parser) throws IOException {
        var embeddingList = parseList(parser, VoyageAIEmbeddingsResponseEntity::parseByteArrayEntry);

        return new InferenceTextEmbeddingBitResults(embeddingList);
    }

    private static InferenceServiceResults parseByteEmbeddingsArray(XContentParser parser) throws IOException {
        var embeddingList = parseList(parser, VoyageAIEmbeddingsResponseEntity::parseByteArrayEntry);

        return new InferenceTextEmbeddingByteResults(embeddingList);
    }

    private static InferenceByteEmbedding parseByteArrayEntry(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        List<Byte> embeddingValuesList = parseList(parser, VoyageAIEmbeddingsResponseEntity::parseEmbeddingInt8Entry);

        return InferenceByteEmbedding.of(embeddingValuesList);
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

    private static InferenceServiceResults parseFloatEmbeddingsArray(XContentParser parser) throws IOException {
        var embeddingList = parseList(parser, VoyageAIEmbeddingsResponseEntity::parseFloatArrayEntry);

        return new InferenceTextEmbeddingFloatResults(embeddingList);
    }

    private static InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding parseFloatArrayEntry(XContentParser parser)
        throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        List<Float> embeddingValuesList = parseList(parser, XContentUtils::parseFloat);
        return InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding.of(embeddingValuesList);
    }

    private VoyageAIEmbeddingsResponseEntity() {}
}
