/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.openai;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.results.TextEmbeddingResults;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.core.Strings.format;

public class OpenAiEmbeddingsResponseEntity {

    /**
     * Parses the OpenAI json response.
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
     *              .... (1536 floats total for ada-002)
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
     *  "model": "text-embedding-ada-002",
     *  "usage": {
     *      "prompt_tokens": 8,
     *      "total_tokens": 8
     *  }
     * }
     * </code>
     * </pre>
     */
    public static TextEmbeddingResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            if (jsonParser.currentToken() == null) {
                jsonParser.nextToken();
            }

            XContentParser.Token token = jsonParser.currentToken();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "data");

            List<TextEmbeddingResults.Embedding> embeddingList = XContentParserUtils.parseList(
                jsonParser,
                OpenAiEmbeddingsResponseEntity::parseEmbeddingObject
            );

            return new TextEmbeddingResults(embeddingList);
        }
    }

    /**
     * Iterates over the tokens until it finds a field name token with the text matching the field requested.
     *
     * @throws IllegalStateException if the field cannot be found
     */
    private static void positionParserAtTokenAfterField(XContentParser parser, String field) throws IOException {
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME && parser.currentName().equals(field)) {
                parser.nextToken();
                return;
            }
        }

        throw new IllegalStateException(format("Failed to find required field [%s] in OpenAI embeddings response", field));
    }

    private static TextEmbeddingResults.Embedding parseEmbeddingObject(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        positionParserAtTokenAfterField(parser, "embedding");

        List<Float> embeddingValues = XContentParserUtils.parseList(parser, OpenAiEmbeddingsResponseEntity::parseEmbeddingList);

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

    private OpenAiEmbeddingsResponseEntity() {}
}
