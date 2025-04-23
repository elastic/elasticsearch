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
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class HuggingFaceChatCompletionResponseEntity {
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in HuggingFace chat response";

    public static ChatCompletionResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            // Ensure the response starts with an array
            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_ARRAY, token, jsonParser);

            // Move to the first object in the array
            token = jsonParser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            // Position the parser at the "generated_text" field
            positionParserAtTokenAfterField(jsonParser, "generated_text", FAILED_TO_FIND_FIELD_TEMPLATE);

            // Extract the "generated_text" value
            XContentParser.Token contentToken = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.VALUE_STRING, contentToken, jsonParser);
            String content = jsonParser.text();

            return new ChatCompletionResults(List.of(new ChatCompletionResults.Result(content)));
        }
    }

    private HuggingFaceChatCompletionResponseEntity() {}
}
