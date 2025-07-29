/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class AnthropicChatCompletionResponseEntity {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Anthropic chat completions response";

    /**
     * Parses the Anthropic chat completion response.
     * For a request like:
     *
     * <pre>
     *     <code>
     *         {
     *             "inputs": ["Please summarize this text: some text"]
     *         }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *  {
     *      "id": "msg_01XzZQmG41BMGe5NZ5p2vEWb",
     *      "type": "message",
     *      "role": "assistant",
     *      "model": "claude-3-opus-20240229",
     *      "content": [
     *          {
     *              "type": "text",
     *              "text": "result"
     *          }
     *      ],
     *      "stop_reason": "end_turn",
     *      "stop_sequence": null,
     *      "usage": {
     *          "input_tokens": 16,
     *          "output_tokens": 326
     *      }
     *  }
     *     </code>
     * </pre>
     */

    public static ChatCompletionResults fromResponse(Request request, HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "content", FAILED_TO_FIND_FIELD_TEMPLATE);

            var completionResults = doParse(jsonParser);

            return new ChatCompletionResults(completionResults);
        }
    }

    private static List<ChatCompletionResults.Result> doParse(XContentParser parser) throws IOException {
        var parsedResults = parseList(parser, (listParser) -> {
            var parsedObject = TextObject.parse(parser);
            // Anthropic also supports a tool_use type, we want to ignore those objects
            if (parsedObject.type == null || parsedObject.type.equals("text") == false || parsedObject.text == null) {
                return null;
            }

            return new ChatCompletionResults.Result(parsedObject.text);
        });

        parsedResults.removeIf(Objects::isNull);
        return parsedResults;
    }

    private record TextObject(@Nullable String type, @Nullable String text) {

        private static final ParseField TEXT = new ParseField("text");
        private static final ParseField TYPE = new ParseField("type");
        private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(
            "anthropic_chat_completions_response",
            true,
            Builder::new
        );

        static {
            PARSER.declareString(Builder::setText, TEXT);
            PARSER.declareString(Builder::setType, TYPE);
        }

        public static TextObject parse(XContentParser parser) throws IOException {
            Builder builder = PARSER.apply(parser, null);
            return builder.build();
        }

        private static final class Builder {

            private String type;
            private String text;

            private Builder() {}

            public Builder setType(String type) {
                this.type = type;
                return this;
            }

            public Builder setText(String text) {
                this.text = text;
                return this;
            }

            public TextObject build() {
                return new TextObject(type, text);
            }
        }
    }
}
