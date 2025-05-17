/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;

public class OpenAiUnifiedStreamingProcessor extends DelegatingProcessor<
    Deque<ServerSentEvent>,
    StreamingUnifiedChatCompletionResults.Results> {
    public static final String FUNCTION_FIELD = "function";
    private static final Logger logger = LogManager.getLogger(OpenAiUnifiedStreamingProcessor.class);

    private static final String CHOICES_FIELD = "choices";
    private static final String DELTA_FIELD = "delta";
    private static final String CONTENT_FIELD = "content";
    private static final String DONE_MESSAGE = "[done]";
    private static final String REFUSAL_FIELD = "refusal";
    private static final String TOOL_CALLS_FIELD = "tool_calls";
    public static final String ROLE_FIELD = "role";
    public static final String FINISH_REASON_FIELD = "finish_reason";
    public static final String INDEX_FIELD = "index";
    public static final String OBJECT_FIELD = "object";
    public static final String MODEL_FIELD = "model";
    public static final String ID_FIELD = "id";
    public static final String CHOICE_FIELD = "choice";
    public static final String USAGE_FIELD = "usage";
    public static final String TYPE_FIELD = "type";
    public static final String NAME_FIELD = "name";
    public static final String ARGUMENTS_FIELD = "arguments";
    public static final String COMPLETION_TOKENS_FIELD = "completion_tokens";
    public static final String PROMPT_TOKENS_FIELD = "prompt_tokens";
    public static final String TOTAL_TOKENS_FIELD = "total_tokens";

    private final BiFunction<String, Exception, Exception> errorParser;

    public OpenAiUnifiedStreamingProcessor(BiFunction<String, Exception, Exception> errorParser) {
        this.errorParser = errorParser;
    }

    @Override
    protected void next(Deque<ServerSentEvent> item) throws Exception {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        var results = new ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk>(item.size());
        for (var event : item) {
            if ("error".equals(event.type()) && event.hasData()) {
                throw errorParser.apply(event.data(), null);
            } else if (event.hasData()) {
                try {
                    var delta = parse(parserConfig, event);
                    delta.forEachRemaining(results::offer);
                } catch (Exception e) {
                    logger.warn("Failed to parse event from inference provider: {}", event);
                    throw errorParser.apply(event.data(), e);
                }
            }
        }

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(new StreamingUnifiedChatCompletionResults.Results(results));
        }
    }

    private static Iterator<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parse(
        XContentParserConfiguration parserConfig,
        ServerSentEvent event
    ) throws IOException {
        if (DONE_MESSAGE.equalsIgnoreCase(event.data())) {
            return Collections.emptyIterator();
        }

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, event.data())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            StreamingUnifiedChatCompletionResults.ChatCompletionChunk chunk = ChatCompletionChunkParser.parse(jsonParser);

            return Collections.singleton(chunk).iterator();
        }
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
            );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(ID_FIELD));
            PARSER.declareObjectArray(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> ChatCompletionChunkParser.ChoiceParser.parse(p),
                new ParseField(CHOICES_FIELD)
            );
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(MODEL_FIELD));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(OBJECT_FIELD));
            PARSER.declareObjectOrNull(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> ChatCompletionChunkParser.UsageParser.parse(p),
                null,
                new ParseField(USAGE_FIELD)
            );
        }

        public static StreamingUnifiedChatCompletionResults.ChatCompletionChunk parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        private static class ChoiceParser {
            private static final ConstructingObjectParser<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice, Void> PARSER =
                new ConstructingObjectParser<>(
                    CHOICE_FIELD,
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
                PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField(CONTENT_FIELD));
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
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(ID_FIELD));
                PARSER.declareObject(
                    ConstructingObjectParser.optionalConstructorArg(),
                    (p, c) -> ChatCompletionChunkParser.FunctionParser.parse(p),
                    new ParseField(FUNCTION_FIELD)
                );
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(TYPE_FIELD));
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
                    FUNCTION_FIELD,
                    true,
                    args -> new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(
                        (String) args[0],
                        (String) args[1]
                    )
                );

            static {
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(ARGUMENTS_FIELD));
                PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField(NAME_FIELD));
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
                    USAGE_FIELD,
                    true,
                    args -> new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage((int) args[0], (int) args[1], (int) args[2])
                );

            static {
                PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField(COMPLETION_TOKENS_FIELD));
                PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField(PROMPT_TOKENS_FIELD));
                PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField(TOTAL_TOKENS_FIELD));
            }

            public static StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage parse(XContentParser parser) throws IOException {
                return PARSER.parse(parser, null);
            }
        }
    }
}
