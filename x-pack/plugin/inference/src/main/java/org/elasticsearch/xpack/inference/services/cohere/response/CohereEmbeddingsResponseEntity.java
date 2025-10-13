/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.response;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.XContentUtils;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;
import static org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType.toLowerCase;

public class CohereEmbeddingsResponseEntity {
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Cohere embeddings response";

    private static final Map<String, CheckedFunction<XContentParser, InferenceServiceResults, IOException>> EMBEDDING_PARSERS = Map.of(
        toLowerCase(CohereEmbeddingType.FLOAT),
        CohereEmbeddingsResponseEntity::parseFloatEmbeddingsArray,
        toLowerCase(CohereEmbeddingType.INT8),
        CohereEmbeddingsResponseEntity::parseByteEmbeddingsArray,
        toLowerCase(CohereEmbeddingType.BINARY),
        CohereEmbeddingsResponseEntity::parseBitEmbeddingsArray
    );
    private static final String VALID_EMBEDDING_TYPES_STRING = supportedEmbeddingTypes();

    private static String supportedEmbeddingTypes() {
        var validTypes = EMBEDDING_PARSERS.keySet().toArray(String[]::new);
        Arrays.sort(validTypes);
        return String.join(", ", validTypes);
    }

    /**
     * Parses the Cohere embed json response.
     * For a request like:
     *
     * <pre>
     * <code>
     * {
     *  "texts": ["hello this is my name", "I wish I was there!"]
     * }
     * </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     * <code>
     * {
     *  "id": "da4f9ea6-37e4-41ab-b5e1-9e2985609555",
     *  "texts": [
     *      "hello",
     *      "awesome"
     *  ],
     *  "embeddings": [
     *      [
     *          123
     *      ],
     *      [
     *          123
     *      ]
     *  ],
     *  "meta": {
     *      "api_version": {
     *          "version": "1"
     *      },
     *      "warnings": [
     *          "default model on embed will be deprecated in the future, please specify a model in the request."
     *      ],
     *      "billed_units": {
     *          "input_tokens": 3
     *      }
     *  },
     *  "response_type": "embeddings_floats"
     * }
     * </code>
     * </pre>
     *
     * Or this:
     *
     * <pre>
     * <code>
     * {
     *  "id": "da4f9ea6-37e4-41ab-b5e1-9e2985609555",
     *  "texts": [
     *      "hello",
     *      "awesome"
     *  ],
     *  "embeddings": {
     *      "float": [
     *          [
     *              123
     *          ],
     *          [
     *              123
     *          ],
     *      ]
     *  },
     *  "meta": {
     *      "api_version": {
     *          "version": "1"
     *      },
     *      "warnings": [
     *          "default model on embed will be deprecated in the future, please specify a model in the request."
     *      ],
     *      "billed_units": {
     *          "input_tokens": 3
     *      }
     *  },
     *  "response_type": "embeddings_floats"
     * }
     * </code>
     * </pre>
     */
    public static InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "embeddings", FAILED_TO_FIND_FIELD_TEMPLATE);

            token = jsonParser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                return parseEmbeddingsObject(jsonParser);
            } else if (token == XContentParser.Token.START_ARRAY) {
                // if the request did not specify the embedding types then it will default to floats
                return parseFloatEmbeddingsArray(jsonParser);
            } else {
                throwUnknownToken(token, jsonParser);
            }

            // This should never be reached. The above code should either return successfully or hit the throwUnknownToken
            // or throw a parsing exception
            throw new IllegalStateException("Reached an invalid state while parsing the Cohere response");
        }
    }

    private static InferenceServiceResults parseEmbeddingsObject(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();

        while (token != null && token != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                var embeddingValueParser = EMBEDDING_PARSERS.get(parser.currentName());
                if (embeddingValueParser != null) {
                    parser.nextToken();
                    return embeddingValueParser.apply(parser);
                }
            }
            token = parser.nextToken();
        }

        throw new IllegalStateException(
            Strings.format(
                "Failed to find a supported embedding type in the Cohere embeddings response. Supported types are [%s]",
                VALID_EMBEDDING_TYPES_STRING
            )
        );
    }

    private static InferenceServiceResults parseBitEmbeddingsArray(XContentParser parser) throws IOException {
        // Cohere returns array of binary embeddings encoded as bytes with int8 precision so we can reuse the byte parser
        var embeddingList = parseList(parser, CohereEmbeddingsResponseEntity::parseByteArrayEntry);

        return new TextEmbeddingBitResults(embeddingList);
    }

    private static InferenceServiceResults parseByteEmbeddingsArray(XContentParser parser) throws IOException {
        var embeddingList = parseList(parser, CohereEmbeddingsResponseEntity::parseByteArrayEntry);

        return new TextEmbeddingByteResults(embeddingList);
    }

    private static TextEmbeddingByteResults.Embedding parseByteArrayEntry(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        List<Byte> embeddingValuesList = parseList(parser, CohereEmbeddingsResponseEntity::parseEmbeddingInt8Entry);

        return TextEmbeddingByteResults.Embedding.of(embeddingValuesList);
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
        var embeddingList = parseList(parser, CohereEmbeddingsResponseEntity::parseFloatArrayEntry);

        return new TextEmbeddingFloatResults(embeddingList);
    }

    private static TextEmbeddingFloatResults.Embedding parseFloatArrayEntry(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        List<Float> embeddingValuesList = parseList(parser, XContentUtils::parseFloat);
        return TextEmbeddingFloatResults.Embedding.of(embeddingValuesList);
    }

    private CohereEmbeddingsResponseEntity() {}
}
