/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.openai;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventField;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;

/**
 * Parses the OpenAI chat completion streaming responses.
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
 *                      "delta": {
 *                          "content": "\n\nHello there, how ",
 *                      },
 *                      "finish_reason": ""
 *                  }
 *              ]
 *          }
 *
 *         {
 *              "id": "chatcmpl-123",
 *              "object": "chat.completion",
 *              "created": 1677652288,
 *              "model": "gpt-3.5-turbo-0613",
 *              "system_fingerprint": "fp_44709d6fcb",
 *              "choices": [
 *                  {
 *                      "index": 1,
 *                      "delta": {
 *                          "content": "may I assist you today?",
 *                      },
 *                      "finish_reason": ""
 *                  }
 *              ]
 *          }
 *
 *         {
 *              "id": "chatcmpl-123",
 *              "object": "chat.completion",
 *              "created": 1677652288,
 *              "model": "gpt-3.5-turbo-0613",
 *              "system_fingerprint": "fp_44709d6fcb",
 *              "choices": [
 *                  {
 *                      "index": 2,
 *                      "delta": {},
 *                      "finish_reason": "stop"
 *                  }
 *              ]
 *          }
 *
 *          [DONE]
 *     </code>
 * </pre>
 */
public class OpenAiStreamingProcessor extends DelegatingProcessor<Deque<ServerSentEvent>, ChunkedToXContent> {
    private static final Logger log = LogManager.getLogger(OpenAiStreamingProcessor.class);
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in OpenAI chat completions response";

    private static final String CHOICES_FIELD = "choices";
    private static final String DELTA_FIELD = "delta";
    private static final String CONTENT_FIELD = "content";
    private static final String DONE_MESSAGE = "[done]";

    @Override
    protected void next(Deque<ServerSentEvent> item) throws Exception {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        var results = new ArrayDeque<StreamingChatCompletionResults.Result>(item.size());
        for (ServerSentEvent event : item) {
            if (ServerSentEventField.DATA == event.name() && event.hasValue()) {
                try {
                    var delta = parse(parserConfig, event);
                    delta.forEachRemaining(results::offer);
                } catch (Exception e) {
                    log.warn("Failed to parse event from inference provider: {}", event);
                    throw e;
                }
            }
        }

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(new StreamingChatCompletionResults.Results(results));
        }
    }

    private Iterator<StreamingChatCompletionResults.Result> parse(XContentParserConfiguration parserConfig, ServerSentEvent event)
        throws IOException {
        if (DONE_MESSAGE.equalsIgnoreCase(event.value())) {
            return Collections.emptyIterator();
        }

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, event.value())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            ChatCompletionChunk chunk = ChatCompletionChunkParser.parse(jsonParser);

            List<StreamingChatCompletionResults.Result> results = new ArrayList<>();
            for (ChatCompletionChunk.Choice choice : chunk.getChoices()) {
                String content = choice.getDelta().getContent();
                String refusal = choice.getDelta().getRefusal();
                List<StreamingChatCompletionResults.ToolCall> toolCalls = parseToolCalls(choice.getDelta().getToolCalls());
                results.add(new StreamingChatCompletionResults.Result(content, refusal, toolCalls));
            }

            return results.iterator();
        }
    }

    private List<StreamingChatCompletionResults.ToolCall> parseToolCalls(List<ChatCompletionChunk.Choice.Delta.ToolCall> toolCalls) {
        List<StreamingChatCompletionResults.ToolCall> parsedToolCalls = new ArrayList<>();
        for (ChatCompletionChunk.Choice.Delta.ToolCall toolCall : toolCalls) {
            int index = toolCall.getIndex();
            String id = toolCall.getId();
            String functionName = toolCall.getFunction() != null ? toolCall.getFunction().getName() : null;
            String functionArguments = toolCall.getFunction() != null ? toolCall.getFunction().getArguments() : null;
            parsedToolCalls.add(new StreamingChatCompletionResults.ToolCall(index, id, functionName, functionArguments));
        }
        return parsedToolCalls;
    }

    public static class ChatCompletionChunk {
        private final String id;
        private List<Choice> choices;
        private final String model;
        private final String object;
        private Usage usage;

        public ChatCompletionChunk(String id, List<Choice> choices, String model, String object, Usage usage) {
            this.id = id;
            this.choices = choices;
            this.model = model;
            this.object = object;
            this.usage = usage;
        }

        public ChatCompletionChunk(String id, Choice[] choices, String model, String object, Usage usage) {
            this.id = id;
            this.choices = Arrays.stream(choices).toList();
            this.model = model;
            this.object = object;
            this.usage = usage;
        }

        public String getId() {
            return id;
        }

        public List<Choice> getChoices() {
            return choices;
        }

        public String getModel() {
            return model;
        }

        public String getObject() {
            return object;
        }

        public Usage getUsage() {
            return usage;
        }

        public static class Choice {
            private final Delta delta;
            private final String finishReason;
            private final int index;

            public Choice(Delta delta, String finishReason, int index) {
                this.delta = delta;
                this.finishReason = finishReason;
                this.index = index;
            }

            public Delta getDelta() {
                return delta;
            }

            public String getFinishReason() {
                return finishReason;
            }

            public int getIndex() {
                return index;
            }

            public static class Delta {
                private final String content;
                private final String refusal;
                private final String role;
                private List<ToolCall> toolCalls;

                public Delta(String content, String refusal, String role, List<ToolCall> toolCalls) {
                    this.content = content;
                    this.refusal = refusal;
                    this.role = role;
                    this.toolCalls = toolCalls;
                }

                public String getContent() {
                    return content;
                }

                public String getRefusal() {
                    return refusal;
                }

                public String getRole() {
                    return role;
                }

                public List<ToolCall> getToolCalls() {
                    return toolCalls;
                }

                public static class ToolCall {
                    private final int index;
                    private final String id;
                    private Function function;
                    private final String type;

                    public ToolCall(int index, String id, Function function, String type) {
                        this.index = index;
                        this.id = id;
                        this.function = function;
                        this.type = type;
                    }

                    public int getIndex() {
                        return index;
                    }

                    public String getId() {
                        return id;
                    }

                    public Function getFunction() {
                        return function;
                    }

                    public String getType() {
                        return type;
                    }

                    public static class Function {
                        private final String arguments;
                        private final String name;

                        public Function(String arguments, String name) {
                            this.arguments = arguments;
                            this.name = name;
                        }

                        public String getArguments() {
                            return arguments;
                        }

                        public String getName() {
                            return name;
                        }
                    }
                }
            }
        }

        public static class Usage {
            private final int completionTokens;
            private final int promptTokens;
            private final int totalTokens;

            public Usage(int completionTokens, int promptTokens, int totalTokens) {
                this.completionTokens = completionTokens;
                this.promptTokens = promptTokens;
                this.totalTokens = totalTokens;
            }

            public int getCompletionTokens() {
                return completionTokens;
            }

            public int getPromptTokens() {
                return promptTokens;
            }

            public int getTotalTokens() {
                return totalTokens;
            }
        }
    }

    public static class ChatCompletionChunkParser {
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ChatCompletionChunk, Void> PARSER = new ConstructingObjectParser<>(
            "chat_completion_chunk",
            true,
            args -> new ChatCompletionChunk(
                (String) args[0],
                (ChatCompletionChunk.Choice[]) args[1],
                (String) args[2],
                (String) args[3],
                (ChatCompletionChunk.Usage) args[4]
            )

            /**
             * TODO
             * Caused by: java.lang.ClassCastException: class java.lang.String cannot be cast to class [Lorg.elasticsearch.xpack.inference.external.openai.OpenAiStreamingProcessor$ChatCompletionChunk$Choice; (java.lang.String is in module java.base of loader 'bootstrap'; [Lorg.elasticsearch.xpack.inference.external.openai.OpenAiStreamingProcessor$ChatCompletionChunk$Choice; is in module org.elasticsearch.inference@9.0.0-SNAPSHOT of loader jdk.internal.loader.Loader @611c3eae)
             *         at org.elasticsearch.inference@9.0.0-SNAPSHOT/org.elasticsearch.xpack.inference.external.openai.OpenAiStreamingProcessor$ChatCompletionChunkParser.lambda$static$0(OpenAiStreamingProcessor.java:354)
             *         at org.elasticsearch.xcontent@9.0.0-SNAPSHOT/org.elasticsearch.xcontent.ConstructingObjectParser.lambda$new$2(ConstructingObjectParser.java:130)
             *         at org.elasticsearch.xcontent@9.0.0-SNAPSHOT/org.elasticsearch.xcontent.ConstructingObjectParser$Target.buildTarget(ConstructingObjectParser.java:555)
             */
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("id"));
            PARSER.declareObjectArray(
                (chunk, choices) -> chunk.choices = choices,
                (p, c) -> ChoiceParser.parse(p),
                new ParseField("choices")
            );
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("model"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("object"));
            PARSER.declareObject((chunk, usage) -> chunk.usage = usage, (p, c) -> UsageParser.parse(p), new ParseField("usage"));
        }

        public static ChatCompletionChunk parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        private static class ChoiceParser {
            private static final ConstructingObjectParser<ChatCompletionChunk.Choice, Void> PARSER = new ConstructingObjectParser<>(
                "choice",
                true,
                args -> new ChatCompletionChunk.Choice((ChatCompletionChunk.Choice.Delta) args[0], (String) args[1], (int) args[2])
            );

            static {
                PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DeltaParser.parse(p), new ParseField("delta"));
                PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("finish_reason"));
                PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField("index"));
            }

            public static ChatCompletionChunk.Choice parse(XContentParser parser) throws IOException {
                return PARSER.apply(parser, null);
            }
        }

        private static class DeltaParser {
            @SuppressWarnings("unchecked")
            private static final ConstructingObjectParser<ChatCompletionChunk.Choice.Delta, Void> PARSER = new ConstructingObjectParser<>(
                "delta",
                true,
                args -> new ChatCompletionChunk.Choice.Delta(
                    (String) args[0],
                    (String) args[1],
                    (String) args[2],
                    (List<ChatCompletionChunk.Choice.Delta.ToolCall>) args[3]
                )
            );

            static {
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("content"));
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("refusal"));
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("role"));
                PARSER.declareObjectArray(
                    (delta, toolCalls) -> delta.toolCalls = toolCalls,
                    (p, c) -> ToolCallParser.parse(p),
                    new ParseField("tool_calls")
                );

            }

            public static ChatCompletionChunk.Choice.Delta parse(XContentParser parser) throws IOException {
                return PARSER.parse(parser, null);
            }
        }

        private static class ToolCallParser {
            private static final ConstructingObjectParser<ChatCompletionChunk.Choice.Delta.ToolCall, Void> PARSER =
                new ConstructingObjectParser<>(
                    "tool_call",
                    true,
                    args -> new ChatCompletionChunk.Choice.Delta.ToolCall(
                        (int) args[0],
                        (String) args[1],
                        (ChatCompletionChunk.Choice.Delta.ToolCall.Function) args[2],
                        (String) args[3]
                    )
                );

            static {
                PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField("index"));
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("id"));
                PARSER.declareObject(
                    (toolCall, function) -> toolCall.function = function,
                    (p, c) -> FunctionParser.parse(p),
                    new ParseField("function")
                );
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("type"));
            }

            return parseList(jsonParser, parser -> {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
            public static ChatCompletionChunk.Choice.Delta.ToolCall parse(XContentParser parser) throws IOException {
                return PARSER.parse(parser, null);
            }
        }

        private static class FunctionParser {
            private static final ConstructingObjectParser<ChatCompletionChunk.Choice.Delta.ToolCall.Function, Void> PARSER =
                new ConstructingObjectParser<>(
                    "function",
                    true,
                    args -> new ChatCompletionChunk.Choice.Delta.ToolCall.Function((String) args[0], (String) args[1])
                );

            static {
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("arguments"));
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("name"));
            }

            public static ChatCompletionChunk.Choice.Delta.ToolCall.Function parse(XContentParser parser) throws IOException {
                return PARSER.parse(parser, null);
            }
        }

        private static class UsageParser {
            private static final ConstructingObjectParser<ChatCompletionChunk.Usage, Void> PARSER = new ConstructingObjectParser<>(
                "usage",
                true,
                args -> new ChatCompletionChunk.Usage((int) args[0], (int) args[1], (int) args[2])
            );

            static {
                PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField("completion_tokens"));
                PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField("prompt_tokens"));
                PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField("total_tokens"));
            }

            public static ChatCompletionChunk.Usage parse(XContentParser parser) throws IOException {
                return PARSER.parse(parser, null);
            }
        }
    }
}
