/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.response;

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

public class AzureOpenAiCompletionResponseEntity {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Azure OpenAI completions response";

    /**
     * Parses the Azure OpenAI completion response.
     * For a request like:
     *
     * <pre>
     *     <code>
     *         {
     *             "inputs": "Please summarize this text: some text"
     *         }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *         {
     *     "choices": [
     *         {
     *             "content_filter_results": {
     *                 "hate": { ... },
     *                 "self_harm": { ... },
     *                 "sexual": { ... },
     *                 "violence": { ... }
     *             },
     *             "finish_reason": "stop",
     *             "index": 0,
     *             "logprobs": null,
     *             "message": {
     *                 "content": "response",
     *                 "role": "assistant"
     *             }
     *         }
     *     ],
     *     "created": 1714982782,
     *     "id": "...",
     *     "model": "gpt-4",
     *     "object": "chat.completion",
     *     "prompt_filter_results": [
     *         {
     *             "prompt_index": 0,
     *             "content_filter_results": {
     *                 "hate": { ... },
     *                 "self_harm": { ... },
     *                 "sexual": { ... },
     *                 "violence": { ... }
     *             }
     *         }
     *     ],
     *     "system_fingerprint": null,
     *     "usage": { ... }
     * }
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
