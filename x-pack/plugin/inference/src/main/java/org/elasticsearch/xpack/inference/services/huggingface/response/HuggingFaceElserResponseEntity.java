/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;

public class HuggingFaceElserResponseEntity {

    /**
     * The response from hugging face will be formatted as [{"token": 0.0...123}]. Each object within the array will correspond to the
     * item within the inputs array within the request sent to hugging face. For example for a request like:
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
     *   <code>
     *     [
     *       {
     *         "the": 0.7226026,
     *         "to": 0.29198948,
     *         "is": 0.059944477,
     *         ...
     *       },
     *       {
     *           "wish": 0.123456,
     *           ...
     *       }
     *     ]
     *   </code>
     * </pre>
     */
    public static SparseEmbeddingResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            var truncationResults = request.getTruncationInfo();
            List<SparseEmbeddingResults.Embedding> parsedEmbeddings = parseList(
                jsonParser,
                (parser, index) -> HuggingFaceElserResponseEntity.parseExpansionResult(truncationResults, parser, index)
            );

            if (parsedEmbeddings.isEmpty()) {
                return new SparseEmbeddingResults(Collections.emptyList());
            }

            return new SparseEmbeddingResults(parsedEmbeddings);
        }
    }

    private static SparseEmbeddingResults.Embedding parseExpansionResult(boolean[] truncationResults, XContentParser parser, int index)
        throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        List<WeightedToken> weightedTokens = new ArrayList<>();
        token = parser.nextToken();
        while (token != null && token != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            var floatToken = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, floatToken, parser);

            weightedTokens.add(new WeightedToken(parser.currentName(), parser.floatValue()));

            token = parser.nextToken();
        }

        // prevent an out of bounds if for some reason the truncation list is smaller than the results
        var isTruncated = truncationResults != null && index < truncationResults.length && truncationResults[index];
        return new SparseEmbeddingResults.Embedding(weightedTokens, isTruncated);
    }

    private HuggingFaceElserResponseEntity() {}
}
