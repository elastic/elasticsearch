/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.huggingface;

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

import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class HuggingFaceEmbeddingsResponseEntity {
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Hugging Face embeddings response";

    /**
     * The response from hugging face will be formatted as <code>{"embeddings": [[0.1, ...], [0.1, ...]}</code>.
     * Each entry in the array will correspond to the entry within the inputs array within the request sent to hugging face. For example
     * for a request like:
     *
     * <pre>
     *     <code>
     *         {
     *             "inputs": ["hello this is my name", "I wish I was there!"]
     *         }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *         {
     *             "embeddings": [
     *                  [
     *                      0.1,
     *                      0.234
     *                  ],
     *                  [
     *                      0.34,
     *                      0.56
     *                  ]
     *             ]
     *         }
     *     </code>
     * </pre>
     */
    public static TextEmbeddingResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "embeddings", FAILED_TO_FIND_FIELD_TEMPLATE);

            List<TextEmbeddingResults.Embedding> embeddingList = XContentParserUtils.parseList(
                jsonParser,
                HuggingFaceEmbeddingsResponseEntity::parseEmbeddingEntry
            );

            return new TextEmbeddingResults(embeddingList);
        }
    }

    private static TextEmbeddingResults.Embedding parseEmbeddingEntry(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);

        List<Float> embeddingValues = XContentParserUtils.parseList(parser, HuggingFaceEmbeddingsResponseEntity::parseEmbeddingList);
        return new TextEmbeddingResults.Embedding(embeddingValues);
    }

    private static float parseEmbeddingList(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
        return parser.floatValue();
    }

    private HuggingFaceEmbeddingsResponseEntity() {}
}
