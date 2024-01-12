/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.cohere;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class CohereEmbeddingsResponseEntity {
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Cohere embeddings response";

    /**
     * Parses the OpenAI json response.
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
     */
    public static TextEmbeddingResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "data", FAILED_TO_FIND_FIELD_TEMPLATE);

            List<TextEmbeddingResults.Embedding> embeddingList = XContentParserUtils.parseList(
                jsonParser,
                CohereEmbeddingsResponseEntity::parseEmbeddingObject
            );

            return new TextEmbeddingResults(embeddingList);
        }
    }

    private static TextEmbeddingResults.Embedding parseEmbeddingObject(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        positionParserAtTokenAfterField(parser, "embedding", FAILED_TO_FIND_FIELD_TEMPLATE);

        List<Float> embeddingValues = XContentParserUtils.parseList(parser, CohereEmbeddingsResponseEntity::parseEmbeddingList);

        // the parser is currently sitting at an ARRAY_END so go to the next token
        parser.nextToken();
        // if there are additional fields within this object, lets skip them, so we can begin parsing the next embedding array
        parser.skipChildren();

        return new TextEmbeddingResults.Embedding(embeddingValues);
    }

    private static float parseEmbeddingList(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
        return parser.floatValue();
    }

    private CohereEmbeddingsResponseEntity() {}
}
