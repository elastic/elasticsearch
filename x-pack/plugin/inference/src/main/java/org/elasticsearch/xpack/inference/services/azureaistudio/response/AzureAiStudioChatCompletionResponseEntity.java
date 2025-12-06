/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.BaseResponseEntity;
import org.elasticsearch.xpack.inference.services.azureaistudio.request.AzureAiStudioChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;

public class AzureAiStudioChatCompletionResponseEntity extends BaseResponseEntity {

    @Override
    protected InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        if (request instanceof AzureAiStudioChatCompletionRequest asChatCompletionRequest) {
            if (asChatCompletionRequest.isRealtimeEndpoint()) {
                return parseRealtimeEndpointResponse(response);
            }

            // we can use the OpenAI chat completion type if it's not a realtime endpoint
            return OpenAiChatCompletionResponseEntity.fromResponse(request, response);
        }

        return null;
    }

    private ChatCompletionResults parseRealtimeEndpointResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            while (token != null && token != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    token = jsonParser.nextToken();
                    continue;
                }

                var currentName = jsonParser.currentName();
                if (currentName == null || currentName.equalsIgnoreCase("output") == false) {
                    token = jsonParser.nextToken();
                    continue;
                }

                token = jsonParser.nextToken();
                ensureExpectedToken(XContentParser.Token.VALUE_STRING, token, jsonParser);
                String content = jsonParser.text();

                return new ChatCompletionResults(List.of(new ChatCompletionResults.Result(content)));
            }

            throw new IllegalStateException("Reached an invalid state while parsing the Azure AI Studio completion response");
        }
    }
}
