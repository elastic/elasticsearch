/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.openai;

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

public class OpenAiChatCompletionResponseEntity {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in OpenAI chat completions response";

    /**
     * Parses the OpenAI chat completion response.
     * For a request like:
     *
     * <pre>
     *     <code>
     *         {
     *             "inputs": ["Please summarize this text: some text", "Answer the following question: Question"]
     *         }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *         {
     *              "id": "chatcmpl-123",
     *              "object": "chat.completion",
     *              "created": 1677652288,
     *              "model": "gpt-3.5-turbo-0613",
     *              "system_fingerprint": "fp_44709d6fcb",
     *              "choices": [
     *                  {
     *                      "index": 0,
     *                      "message": {
     *                          "role": "assistant",
     *                          "content": "\n\nHello there, how may I assist you today?",
    *                          },
     *                      "logprobs": null,
     *                      "finish_reason": "stop"
     *                  }
     *              ],
     *              "usage": {
     *                "prompt_tokens": 9,
     *                "completion_tokens": 12,
     *                "total_tokens": 21
     *              }
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

            positionParserAtTokenAfterField(jsonParser, "choices", FAILED_TO_FIND_FIELD_TEMPLATE);

            jsonParser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, jsonParser.currentToken(), jsonParser);

            positionParserAtTokenAfterField(jsonParser, "message", FAILED_TO_FIND_FIELD_TEMPLATE);

            token = jsonParser.currentToken();

            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "content", FAILED_TO_FIND_FIELD_TEMPLATE);

            XContentParser.Token contentToken = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.VALUE_STRING, contentToken, jsonParser);
            String content = jsonParser.text();

            return new ChatCompletionResults(List.of(new ChatCompletionResults.Result(content)));
        }
    }

}
