/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.googlevertexai;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.consumeUntilObjectEnd;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterFieldCurrentFlatObj;

public class GoogleVertexAiRerankResponseEntity {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Google Vertex AI rerank response";

    /**
     * Parses the Google Vertex AI rerank response.
     *
     * For a request like:
     *
     * <pre>
     *     <code>
     *         {
     *              "query": "some query",
     *              "records": [
     *                  {
     *                      "id": "1",
     *                      "title": "title 1",
     *                      "content": "content 1"
     *                  },
     *                  {
     *                      "id": "2",
     *                      "title": "title 2",
     *                      "content": "content 2"
     *                  }
     *     ]
     * }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *         {
     *              "records": [
     *                  {
     *                      "id": "2",
     *                      "title": "title 2",
     *                      "content": "content 2",
     *                      "score": 0.97
     *                  },
     *                  {
     *                      "id": "1",
     *                      "title": "title 1",
     *                      "content": "content 1",
     *                      "score": 0.18
     *                  }
     *             ]
     *         }
     *     </code>
     * </pre>
     */

    public static RankedDocsResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "records", FAILED_TO_FIND_FIELD_TEMPLATE);

            List<RankedDocsResults.RankedDoc> rankedDocs = parseList(jsonParser, GoogleVertexAiRerankResponseEntity::parseRankedDoc);

            return new RankedDocsResults(rankedDocs);
        }
    }

    private static RankedDocsResults.RankedDoc parseRankedDoc(XContentParser parser, Integer index) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        positionParserAtTokenAfterFieldCurrentFlatObj(parser, "content", FAILED_TO_FIND_FIELD_TEMPLATE);
        XContentParser.Token token = parser.currentToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, token, parser);
        String content = parser.text();

        positionParserAtTokenAfterFieldCurrentFlatObj(parser, "score", FAILED_TO_FIND_FIELD_TEMPLATE);
        token = parser.currentToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
        float score = parser.floatValue();

        consumeUntilObjectEnd(parser);

        return new RankedDocsResults.RankedDoc(index, score, content);
    }
}
