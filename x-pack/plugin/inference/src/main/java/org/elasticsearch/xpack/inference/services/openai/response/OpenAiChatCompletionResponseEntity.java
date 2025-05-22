/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.response;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class OpenAiChatCompletionResponseEntity {

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
        try (var p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, response.body())) {
            return CompletionResult.PARSER.apply(p, null).toChatCompletionResults();
        }
    }

    public record CompletionResult(List<Choice> choices) {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<CompletionResult, Void> PARSER = new ConstructingObjectParser<>(
            CompletionResult.class.getSimpleName(),
            true,
            args -> new CompletionResult((List<Choice>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), Choice.PARSER::apply, new ParseField("choices"));
        }

        public ChatCompletionResults toChatCompletionResults() {
            return new ChatCompletionResults(
                choices.stream().map(choice -> new ChatCompletionResults.Result(choice.message.content)).toList()
            );
        }
    }

    public record Choice(Message message) {
        public static final ConstructingObjectParser<Choice, Void> PARSER = new ConstructingObjectParser<>(
            Choice.class.getSimpleName(),
            true,
            args -> new Choice((Message) args[0])
        );

        static {
            PARSER.declareObject(constructorArg(), Message.PARSER::apply, new ParseField("message"));
        }
    }

    public record Message(String content) {
        public static final ConstructingObjectParser<Message, Void> PARSER = new ConstructingObjectParser<>(
            Message.class.getSimpleName(),
            true,
            args -> new Message((String) args[0])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("content"));
        }
    }
}
