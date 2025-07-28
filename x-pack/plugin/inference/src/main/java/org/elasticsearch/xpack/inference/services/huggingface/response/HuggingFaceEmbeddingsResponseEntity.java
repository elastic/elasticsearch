/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.XContentUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class HuggingFaceEmbeddingsResponseEntity {
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Hugging Face embeddings response";

    /**
     * Parse the response from hugging face. The known formats are an array of arrays and object with an {@code embeddings} field containing
     * an array of arrays.
     */
    public static TextEmbeddingFloatResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            if (token == XContentParser.Token.START_ARRAY) {
                return parseArrayFormat(jsonParser);
            } else if (token == XContentParser.Token.START_OBJECT) {
                return parseObjectFormat(jsonParser);
            } else {
                throwUnknownToken(token, jsonParser);
            }
        }

        // This should never be reached. The above code should either return successfully or hit the throwUnknownToken
        // or throw a parsing exception
        throw new IllegalStateException("Reached an invalid state while parsing the hugging face response");
    }

    /**
     * The response from hugging face could be formatted as <code>[[0.1, ...], [0.1, ...]]</code>.
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
     *         [
     *              [
     *                  0.1,
     *                  0.234
     *              ],
     *              [
     *                  0.34,
     *                  0.56
     *              ]
     *         ]
     *     </code>
     * </pre>
     *
     * Example models with this response format:
     * <a href="https://huggingface.co/intfloat/e5-small-v2">intfloat/e5-small-v2</a>
     * <a href="https://huggingface.co/intfloat/e5-base-v2">intfloat/e5-base-v2</a>
     * <a href="https://huggingface.co/intfloat/multilingual-e5-base">intfloat/multilingual-e5-base</a>
     * <a href="https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2">sentence-transformers/all-MiniLM-L6-v2</a>
     * <a href="https://huggingface.co/sentence-transformers/all-MiniLM-L12-v2">sentence-transformers/all-MiniLM-L12-v2</a>
     */
    private static TextEmbeddingFloatResults parseArrayFormat(XContentParser parser) throws IOException {
        List<TextEmbeddingFloatResults.Embedding> embeddingList = parseList(
            parser,
            HuggingFaceEmbeddingsResponseEntity::parseEmbeddingEntry
        );

        return new TextEmbeddingFloatResults(embeddingList);
    }

    /**
     * The response from hugging face could be formatted as <code>{"embeddings": [[0.1, ...], [0.1, ...]}</code>.
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
     *
     * Example models with this response format:
     * <a href="https://huggingface.co/intfloat/multilingual-e5-small">intfloat/multilingual-e5-small</a>
     * <a href="https://huggingface.co/sentence-transformers/all-mpnet-base-v2">sentence-transformers/all-mpnet-base-v2</a>
     */
    private static TextEmbeddingFloatResults parseObjectFormat(XContentParser parser) throws IOException {
        positionParserAtTokenAfterField(parser, "embeddings", FAILED_TO_FIND_FIELD_TEMPLATE);

        List<TextEmbeddingFloatResults.Embedding> embeddingList = parseList(
            parser,
            HuggingFaceEmbeddingsResponseEntity::parseEmbeddingEntry
        );

        return new TextEmbeddingFloatResults(embeddingList);
    }

    private static TextEmbeddingFloatResults.Embedding parseEmbeddingEntry(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);

        List<Float> embeddingValuesList = parseList(parser, XContentUtils::parseFloat);
        return TextEmbeddingFloatResults.Embedding.of(embeddingValuesList);
    }

    private HuggingFaceEmbeddingsResponseEntity() {}
}
