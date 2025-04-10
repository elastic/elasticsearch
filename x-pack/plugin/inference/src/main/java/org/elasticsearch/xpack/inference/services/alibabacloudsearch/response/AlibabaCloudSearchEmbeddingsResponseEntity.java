/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.response;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class AlibabaCloudSearchEmbeddingsResponseEntity extends AlibabaCloudSearchResponseEntity {
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE =
        "Failed to find required field [%s] in AlibabaCloud Search embeddings response";

    /**
     * Parses the AlibabaCloud Search embedding json response.
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
     *     "request_id": "B4AB89C8-B135-xxxx-A6F8-2BAB801A2CE4",
     *     "latency": 38,
     *     "usage": {
     *         "token_count": 3072
     *     },
     *     "result": {
     *         "embeddings": [
     *             {
     *                 "index": 0,
     *                 "embedding": [
     *                     -0.02868066355586052,
     *                     0.022033605724573135,
     *                     -0.0417383536696434,
     *                     -0.044081952422857285,
     *                     0.02141784131526947,
     *                     -8.240503375418484E-4,
     *                     -0.01309406291693449,
     *                     -0.02169642224907875,
     *                     -0.03996409475803375,
     *                     0.008053945377469063,
     *                     ...
     *                     -0.05131729692220688,
     *                     -0.016595875844359398
     *                 ]
     *             }
     *         ]
     *     }
     * }
     * </code>
     * </pre>
     */
    public static TextEmbeddingFloatResults fromResponse(Request request, HttpResult response) throws IOException {
        return fromResponse(request, response, parser -> {
            positionParserAtTokenAfterField(parser, "embeddings", FAILED_TO_FIND_FIELD_TEMPLATE);

            List<TextEmbeddingFloatResults.Embedding> embeddingList = XContentParserUtils.parseList(
                parser,
                AlibabaCloudSearchEmbeddingsResponseEntity::parseEmbeddingObject
            );

            return new TextEmbeddingFloatResults(embeddingList);
        });
    }

    private static TextEmbeddingFloatResults.Embedding parseEmbeddingObject(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        positionParserAtTokenAfterField(parser, "embedding", FAILED_TO_FIND_FIELD_TEMPLATE);

        List<Float> embeddingValues = XContentParserUtils.parseList(parser, AlibabaCloudSearchEmbeddingsResponseEntity::parseEmbeddingList);

        // the parser is currently sitting at an ARRAY_END so go to the next token
        parser.nextToken();
        // if there are additional fields within this object, lets skip them, so we can begin parsing the next embedding array
        parser.skipChildren();

        return TextEmbeddingFloatResults.Embedding.of(embeddingValues);
    }

    private static float parseEmbeddingList(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
        return parser.floatValue();
    }

    private AlibabaCloudSearchEmbeddingsResponseEntity() {}
}
