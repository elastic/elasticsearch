/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.response;

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

public class CohereCompletionResponseEntity {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Cohere chat response";

    /**
     * Parses the Cohere chat json response.
     * For a request like:
     *
     * <pre>
     *     <code>
     *         {
     *            "message": "What is Elastic?"
     *         }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *         {
     *              "response_id": "some id",
     *              "text": "response",
     *              "generation_id": "some id",
     *              "chat_history": [
     *                               {
     *                                  "role": "USER",
     *                                  "message": "What is Elastic?"
     *                               },
     *                               {
     *                                  "role": "CHATBOT",
     *                                  "message": "response"
     *                               }
     *               ],
     *              "finish_reason": "COMPLETE",
     *              "meta": {
     *                  "api_version": {
     *                      "version": "1"
     *                  },
     *              "billed_units": {
     *                      "input_tokens": 4,
     *                      "output_tokens": 229
     *                  },
     *              "tokens": {
     *                      "input_tokens": 70,
     *                      "output_tokens": 229
     *                  }
     *             }
     *          }
     *     </code>
     * </pre>
     */

    public static ChatCompletionResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "text", FAILED_TO_FIND_FIELD_TEMPLATE);

            XContentParser.Token contentToken = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.VALUE_STRING, contentToken, jsonParser);
            String content = jsonParser.text();

            return new ChatCompletionResults(List.of(new ChatCompletionResults.Result(content)));
        }
    }
}
