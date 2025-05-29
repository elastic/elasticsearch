/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class ElasticInferenceServiceSparseEmbeddingsResponseEntity {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE =
        "Failed to find required field [%s] in Elastic Inference Service embeddings response";

    /**
     * Parses the Elastic Inference Service json response.
     *
     * For a request like:
     *
     * <pre>
     *     <code>
     *         {
     *             "inputs": ["Embed this text", "Embed this text, too"]
     *         }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *         {
     *           "data": [
     *                     {
     *                       "Embed": 2.1259406,
     *                       "this": 1.7073475,
     *                       "text": 0.9020516
     *                     },
     *                    (...)
     *                  ],
     *           "meta": {
     *               "processing_latency": ...,
     *               "request_time": ...
     *           }
     *     </code>
     * </pre>
     */

    public static SparseEmbeddingResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "data", FAILED_TO_FIND_FIELD_TEMPLATE);

            var truncationResults = request.getTruncationInfo();
            List<SparseEmbeddingResults.Embedding> parsedEmbeddings = parseList(
                jsonParser,
                (parser, index) -> ElasticInferenceServiceSparseEmbeddingsResponseEntity.parseExpansionResult(
                    truncationResults,
                    parser,
                    index
                )
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

    private ElasticInferenceServiceSparseEmbeddingsResponseEntity() {}
}
