/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.googleaistudio;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class GoogleAiStudioCompletionResponseEntity {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE =
        "Failed to find required field [%s] in Google AI Studio completion response";

    /**
     * Parses the Google AI Studio completion response.
     *
     * For a request like:
     *
     * <pre>
     *     <code>
     *         {
     *           "contents": [
     *                          {
     *                              "parts": [{
     *                                  "text": "input"
     *                              }]
     *                          }
     *                      ]
     *          }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *         {
     *     "candidates": [
     *         {
     *             "content": {
     *                 "parts": [
     *                     {
     *                         "text": "response"
     *                     }
     *                 ],
     *                 "role": "model"
     *             },
     *             "finishReason": "STOP",
     *             "index": 0,
     *             "safetyRatings": [...]
     *         }
     *     ],
     *     "usageMetadata": { ... }
     * }
     *     </code>
     * </pre>
     *
     */

    public static ChatCompletionResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            return new ChatCompletionResults(List.of(new ChatCompletionResults.Result(content(jsonParser))));
        }
    }

    public static String content(XContentParser jsonParser) throws IOException {
        moveToFirstToken(jsonParser);

        XContentParser.Token token = jsonParser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

        positionParserAtTokenAfterField(jsonParser, "candidates", FAILED_TO_FIND_FIELD_TEMPLATE);

        jsonParser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, jsonParser.currentToken(), jsonParser);

        positionParserAtTokenAfterField(jsonParser, "content", FAILED_TO_FIND_FIELD_TEMPLATE);

        token = jsonParser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

        positionParserAtTokenAfterField(jsonParser, "parts", FAILED_TO_FIND_FIELD_TEMPLATE);

        jsonParser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

        positionParserAtTokenAfterField(jsonParser, "text", FAILED_TO_FIND_FIELD_TEMPLATE);

        XContentParser.Token contentToken = jsonParser.currentToken();
        ensureExpectedToken(XContentParser.Token.VALUE_STRING, contentToken, jsonParser);
        return jsonParser.text();

    }
}
