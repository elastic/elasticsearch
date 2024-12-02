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
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventField;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;

public class OpenAiUnifiedStreamingProcessor extends DelegatingProcessor<Deque<ServerSentEvent>, ChunkedToXContent> {
    private static final Logger logger = LogManager.getLogger(OpenAiUnifiedStreamingProcessor.class);
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in OpenAI chat completions response";

    private static final String CHOICES_FIELD = "choices";
    private static final String DELTA_FIELD = "delta";
    private static final String CONTENT_FIELD = "content";
    private static final String DONE_MESSAGE = "[done]";
    private static final String REFUSAL_FIELD = "refusal";
    private static final String TOOL_CALLS_FIELD = "tool_calls";
    public static final String ROLE_FIELD = "role";
    public static final String FINISH_REASON_FIELD = "finish_reason";
    public static final String INDEX_FIELD = "index";

    @Override
    protected void next(Deque<ServerSentEvent> item) throws Exception {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        var results = new ArrayDeque<StreamingUnifiedChatCompletionResults.Result>(item.size());
        for (ServerSentEvent event : item) {
            if (ServerSentEventField.DATA == event.name() && event.hasValue()) {
                try {
                    var delta = parse(parserConfig, event);
                    delta.forEachRemaining(results::offer);
                } catch (Exception e) {
                    logger.warn("Failed to parse event from inference provider: {}", event);
                    throw e;
                }
            }
        }

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(new StreamingUnifiedChatCompletionResults.Results(results));
        }
    }

    private Iterator<StreamingUnifiedChatCompletionResults.Result> parse(XContentParserConfiguration parserConfig, ServerSentEvent event)
        throws IOException {
        if (DONE_MESSAGE.equalsIgnoreCase(event.value())) {
            return Collections.emptyIterator();
        }

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, event.value())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            StreamingUnifiedChatCompletionResults.ChatCompletionChunk chunk = ChatCompletionChunkParser.parse(jsonParser);

            List<StreamingUnifiedChatCompletionResults.Result> results = new ArrayList<>();
            for (StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice choice : chunk.getChoices()) {
                String content = choice.getDelta().getContent();
                String refusal = choice.getDelta().getRefusal();
                List<StreamingUnifiedChatCompletionResults.ToolCall> toolCalls = parseToolCalls(choice.getDelta().getToolCalls());
                results.add(
                    new StreamingUnifiedChatCompletionResults.Result(
                        content,
                        refusal,
                        toolCalls,
                        choice.getFinishReason(),
                        chunk.getModel(),
                        chunk.getObject(),
                        chunk.getUsage()
                    )
                );
            }

            return results.iterator();
        }
    }

    private List<StreamingUnifiedChatCompletionResults.ToolCall> parseToolCalls(
        List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall> toolCalls
    ) {
        List<StreamingUnifiedChatCompletionResults.ToolCall> parsedToolCalls = new ArrayList<>();

        if (toolCalls == null || toolCalls.isEmpty()) {
            return null;
        }

        for (StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall toolCall : toolCalls) {
            int index = toolCall.getIndex();
            String id = toolCall.getId();
            String functionName = toolCall.getFunction() != null ? toolCall.getFunction().getName() : null;
            String functionArguments = toolCall.getFunction() != null ? toolCall.getFunction().getArguments() : null;
            parsedToolCalls.add(new StreamingUnifiedChatCompletionResults.ToolCall(index, id, functionName, functionArguments));
        }
        return parsedToolCalls;
    }

    public static class ChatCompletionChunkParser {
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<StreamingUnifiedChatCompletionResults.ChatCompletionChunk, Void> PARSER =
            new ConstructingObjectParser<>(
                "chat_completion_chunk",
                true,
                args -> new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(
                    (String) args[0],
                    (List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice>) args[1],
                    (String) args[2],
                    (String) args[3],
                    (StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage) args[4]
                )

                /**
                 * TODO
                 * Caused by: java.lang.ClassCastException: class java.lang.String cannot be cast to class
                 * [Lorg.elasticsearch.xpack.inference.external.openai.OpenAiStreamingProcessor$ChatCompletionChunk$Choice;
                 * (java.lang.String is in module java.base of loader 'bootstrap'; [Lorg.elasticsearch.xpack.inference.external.openai.
                 * OpenAiStreamingProcessor$ChatCompletionChunk$Choice; is in module org.elasticsearch.inference@9.0.0-SNAPSHOT of loader
                 * jdk.internal.loader.Loader @611c3eae)
                 *         at org.elasticsearch.inference@9.0.0-SNAPSHOT/org.elasticsearch.xpack.inference.external.openai.
                 *         OpenAiStreamingProcessor$ChatCompletionChunkParser.lambda$static$0(OpenAiStreamingProcessor.java:354)
                 *         at org.elasticsearch.xcontent@9.0.0-SNAPSHOT/org.elasticsearch.xcontent.ConstructingObjectParser.
                 *         lambda$new$2(ConstructingObjectParser.java:130)
                 *         at org.elasticsearch.xcontent@9.0.0-SNAPSHOT/org.elasticsearch.xcontent.ConstructingObjectParser$Target.
                 *         buildTarget(ConstructingObjectParser.java:555)
                 */
            );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("id"));
            PARSER.declareObjectArray(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> ChatCompletionChunkParser.ChoiceParser.parse(p),
                new ParseField(CHOICES_FIELD)
            );
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("model"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("object"));
            PARSER.declareObject(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> ChatCompletionChunkParser.UsageParser.parse(p),
                new ParseField("usage")
            );
        }

        public static StreamingUnifiedChatCompletionResults.ChatCompletionChunk parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        private static class ChoiceParser {
            private static final ConstructingObjectParser<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice, Void> PARSER =
                new ConstructingObjectParser<>(
                    "choice",
                    true,
                    args -> new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(
                        (StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta) args[0],
                        (String) args[1],
                        (int) args[2]
                    )
                );

            static {
                PARSER.declareObject(
                    ConstructingObjectParser.constructorArg(),
                    (p, c) -> ChatCompletionChunkParser.DeltaParser.parse(p),
                    new ParseField(DELTA_FIELD)
                );
                PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField(FINISH_REASON_FIELD));
                PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField(INDEX_FIELD));
            }

            public static StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice parse(XContentParser parser) {
                return PARSER.apply(parser, null);
            }
        }

        private static class DeltaParser {
            @SuppressWarnings("unchecked")
            private static final ConstructingObjectParser<
                StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta,
                Void> PARSER = new ConstructingObjectParser<>(
                    DELTA_FIELD,
                    true,
                    args -> new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(
                        (String) args[0],
                        (String) args[1],
                        (String) args[2],
                        (List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall>) args[3]
                    )
                );

            static {
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(CONTENT_FIELD));
                PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField(REFUSAL_FIELD));
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(ROLE_FIELD));
                PARSER.declareObjectArray(
                    ConstructingObjectParser.optionalConstructorArg(),
                    (p, c) -> ChatCompletionChunkParser.ToolCallParser.parse(p),
                    new ParseField(TOOL_CALLS_FIELD)
                );
            }

            public static StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta parse(XContentParser parser)
                throws IOException {
                return PARSER.parse(parser, null);
            }
        }

        private static class ToolCallParser {
            private static final ConstructingObjectParser<
                StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall,
                Void> PARSER = new ConstructingObjectParser<>(
                    "tool_call",
                    true,
                    args -> new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
                        (int) args[0],
                        (String) args[1],
                        (StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function) args[2],
                        (String) args[3]
                    )
                );

            static {
                PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField(INDEX_FIELD));
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("id"));
                PARSER.declareObject(
                    (toolCall, function) -> toolCall.function = function,
                    (p, c) -> ChatCompletionChunkParser.FunctionParser.parse(p),
                    new ParseField("function")
                );
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("type"));
            }

            public static StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall parse(XContentParser parser)
                throws IOException {
                return PARSER.parse(parser, null);
            }
        }

        private static class FunctionParser {
            private static final ConstructingObjectParser<
                StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function,
                Void> PARSER = new ConstructingObjectParser<>(
                    "function",
                    true,
                    args -> new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(
                        (String) args[0],
                        (String) args[1]
                    )
                );

            static {
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("arguments"));
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("name"));
            }

            public static StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function parse(
                XContentParser parser
            ) throws IOException {
                return PARSER.parse(parser, null);
            }
        }

        private static class UsageParser {
            private static final ConstructingObjectParser<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage, Void> PARSER =
                new ConstructingObjectParser<>(
                    "usage",
                    true,
                    args -> new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage((int) args[0], (int) args[1], (int) args[2])
                );

            static {
                PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField("completion_tokens"));
                PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField("prompt_tokens"));
                PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField("total_tokens"));
            }

            public static StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage parse(XContentParser parser) throws IOException {
                return PARSER.parse(parser, null);
            }
        }
    }
}
