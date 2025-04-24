/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.consumeUntilObjectEnd;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class GoogleVertexAiEmbeddingsResponseEntity {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE =
        "Failed to find required field [%s] in Google Vertex AI embeddings response";

    /**
     * Parses the Google Vertex AI get embeddings response.
     * For a request like:
     *
     * <pre>
     *     <code>
     *         {
     *             "inputs": ["Embed this", "Embed this, too"]
     *         }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *         {
     *           "predictions": [
     *              {
     *                "embeddings": {
     *                  "statistics": {
     *                    "truncated": false,
     *                    "token_count": 6
     *                  },
     *                  "values": [ ... ]
     *                }
     *              }
     *           ]
     *         }
     *     </code>
     * </pre>
     */

    public static TextEmbeddingFloatResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "predictions", FAILED_TO_FIND_FIELD_TEMPLATE);

            List<TextEmbeddingFloatResults.Embedding> embeddingList = parseList(
                jsonParser,
                GoogleVertexAiEmbeddingsResponseEntity::parseEmbeddingObject
            );

            return new TextEmbeddingFloatResults(embeddingList);
        }
    }

    private static TextEmbeddingFloatResults.Embedding parseEmbeddingObject(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        positionParserAtTokenAfterField(parser, "embeddings", FAILED_TO_FIND_FIELD_TEMPLATE);

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        positionParserAtTokenAfterField(parser, "values", FAILED_TO_FIND_FIELD_TEMPLATE);

        List<Float> embeddingValueList = parseList(parser, GoogleVertexAiEmbeddingsResponseEntity::parseEmbeddingList);

        // parse and discard the rest of the two objects
        consumeUntilObjectEnd(parser);
        consumeUntilObjectEnd(parser);

        return TextEmbeddingFloatResults.Embedding.of(embeddingValueList);
    }

    private static float parseEmbeddingList(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
        return parser.floatValue();
    }

    private GoogleVertexAiEmbeddingsResponseEntity() {}
}
