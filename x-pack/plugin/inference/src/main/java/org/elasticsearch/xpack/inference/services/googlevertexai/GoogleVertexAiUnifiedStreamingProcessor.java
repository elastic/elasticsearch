/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;

public class GoogleVertexAiUnifiedStreamingProcessor extends DelegatingProcessor<
    Deque<ServerSentEvent>,
    StreamingUnifiedChatCompletionResults.Results> {

    private static final Logger logger = LogManager.getLogger(GoogleVertexAiUnifiedStreamingProcessor.class);

    private static final String CANDIDATES_FIELD = "candidates";
    private static final String CONTENT_FIELD = "content";
    private static final String ROLE_FIELD = "role";
    private static final String PARTS_FIELD = "parts";
    private static final String TEXT_FIELD = "text";
    private static final String FINISH_REASON_FIELD = "finishReason";
    private static final String INDEX_FIELD = "index";
    private static final String USAGE_METADATA_FIELD = "usageMetadata";
    private static final String PROMPT_TOKEN_COUNT_FIELD = "promptTokenCount";
    private static final String CANDIDATES_TOKEN_COUNT_FIELD = "candidatesTokenCount";
    private static final String TOTAL_TOKEN_COUNT_FIELD = "totalTokenCount";
    private static final String MODEL_VERSION_FIELD = "modelVersion";
    private static final String RESPONSE_ID_FIELD = "responseId";
    private static final String FUNCTION_CALL_FIELD = "functionCall";
    private static final String FUNCTION_NAME_FIELD = "name";
    private static final String FUNCTION_ARGS_FIELD = "args";

    private static final String CHAT_COMPLETION_CHUNK = "chat.completion.chunk";
    private static final String FUNCTION_TYPE = "function";

    private final BiFunction<String, Exception, Exception> errorParser;

    public GoogleVertexAiUnifiedStreamingProcessor(BiFunction<String, Exception, Exception> errorParser) {
        this.errorParser = errorParser;
    }

    @Override
    protected void next(Deque<ServerSentEvent> events) throws Exception {

        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var results = new ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk>(events.size());

        for (var event : events) {
            try {
                var completionChunk = parse(parserConfig, event.data());
                completionChunk.forEachRemaining(results::offer);
            } catch (Exception e) {
                var eventString = event.data();
                logger.warn("Failed to parse event from Google Vertex AI provider: {}", eventString);
                throw errorParser.apply(eventString, e);
            }
        }

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(new StreamingUnifiedChatCompletionResults.Results(results));
        }
    }

    private Iterator<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parse(
        XContentParserConfiguration parserConfig,
        String event
    ) throws IOException {
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, event)) {
            moveToFirstToken(jsonParser);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, jsonParser.currentToken(), jsonParser);

            StreamingUnifiedChatCompletionResults.ChatCompletionChunk chunk = GoogleVertexAiChatCompletionChunkParser.parse(jsonParser);
            return Collections.singleton(chunk).iterator();
        }
    }

    public static class GoogleVertexAiChatCompletionChunkParser {
        private static @Nullable StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage usageMetadataToChunk(
            @Nullable UsageMetadata usage
        ) {
            if (usage == null) {
                return null;
            }
            return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(
                usage.candidatesTokenCount(),
                usage.promptTokenCount(),
                usage.totalTokenCount()
            );
        }

        private static StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice candidateToChoice(Candidate candidate) {
            StringBuilder contentTextBuilder = new StringBuilder();
            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall> toolCalls = new ArrayList<>();

            String role = null;

            var contentAndPartsAreNotEmpty = candidate.content() != null
                && candidate.content().parts() != null
                && candidate.content().parts().isEmpty() == false;

            if (contentAndPartsAreNotEmpty) {
                role = candidate.content().role(); // Role is at the content level
                for (Part part : candidate.content().parts()) {
                    if (part.text() != null) {
                        contentTextBuilder.append(part.text());
                    }
                    if (part.functionCall() != null) {
                        FunctionCall fc = part.functionCall();
                        var function = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(
                            fc.args(),
                            fc.name()
                        );
                        toolCalls.add(
                            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
                                0, // No explicit ID from VertexAI so we use 0
                                function.name(), // VertexAI does not provide an id for the function call so we use the name
                                function,
                                FUNCTION_TYPE
                            )
                        );
                    }
                }
            }

            List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall> finalToolCalls = toolCalls.isEmpty()
                ? null
                : toolCalls;

            var delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(
                contentTextBuilder.isEmpty() ? null : contentTextBuilder.toString(),
                null,
                role,
                finalToolCalls
            );

            return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, candidate.finishReason(), candidate.index());
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<StreamingUnifiedChatCompletionResults.ChatCompletionChunk, Void> PARSER =
            new ConstructingObjectParser<>("google_vertexai_chat_completion_chunk", true, args -> {
                List<Candidate> candidates = (List<Candidate>) args[0];
                UsageMetadata usage = (UsageMetadata) args[1];
                String modelversion = (String) args[2];
                String responseId = (String) args[3];

                boolean candidatesIsEmpty = candidates == null || candidates.isEmpty();
                List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice> choices = candidatesIsEmpty
                    ? Collections.emptyList()
                    : candidates.stream().map(GoogleVertexAiChatCompletionChunkParser::candidateToChoice).toList();

                return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(
                    responseId,
                    choices,
                    modelversion,
                    CHAT_COMPLETION_CHUNK,
                    usageMetadataToChunk(usage)
                );
            });

        static {
            PARSER.declareObjectArray(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> CandidateParser.parse(p),
                new ParseField(CANDIDATES_FIELD)
            );
            PARSER.declareObject(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> UsageMetadataParser.parse(p),
                new ParseField(USAGE_METADATA_FIELD)
            );
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(MODEL_VERSION_FIELD));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(RESPONSE_ID_FIELD));
        }

        public static StreamingUnifiedChatCompletionResults.ChatCompletionChunk parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    // --- Nested Parsers for Google Vertex AI structure ---

    private record Candidate(Content content, String finishReason, int index) {}

    private static class CandidateParser {
        private static final ConstructingObjectParser<Candidate, Void> PARSER = new ConstructingObjectParser<>("candidate", true, args -> {
            var content = (Content) args[0];
            var finishReason = (String) args[1];
            var index = args[2] == null ? 0 : (int) args[2];
            return new Candidate(content, finishReason, index);
        });

        static {
            PARSER.declareObject(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> ContentParser.parse(p),
                new ParseField(CONTENT_FIELD)
            );
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(FINISH_REASON_FIELD));
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(INDEX_FIELD));
        }

        public static Candidate parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    private record Content(String role, List<Part> parts) {}

    private static class ContentParser {
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Content, Void> PARSER = new ConstructingObjectParser<>(
            CONTENT_FIELD,
            true,
            args -> new Content((String) args[0], (List<Part>) args[1])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(ROLE_FIELD));
            PARSER.declareObjectArray(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> PartParser.parse(p),
                new ParseField(PARTS_FIELD)
            );
        }

        public static Content parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    private record Part(@Nullable String text, @Nullable FunctionCall functionCall) {} // Modified

    private static class PartParser {
        private static final ConstructingObjectParser<Part, Void> PARSER = new ConstructingObjectParser<>(
            "part",
            true,
            args -> new Part((String) args[0], (FunctionCall) args[1])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(TEXT_FIELD));
            PARSER.declareObject(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> FunctionCallParser.parse(p),
                new ParseField(FUNCTION_CALL_FIELD)
            );
        }

        public static Part parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    private record FunctionCall(String name, String args) {}

    private static class FunctionCallParser {
        private static final ConstructingObjectParser<FunctionCall, Void> PARSER = new ConstructingObjectParser<>(
            FUNCTION_CALL_FIELD,
            true,
            args -> {
                var name = (String) args[0];

                @SuppressWarnings("unchecked")
                var argsMap = (Map<String, String>) args[1];
                if (argsMap == null) {
                    return new FunctionCall(name, null);
                }
                try {
                    var builder = XContentFactory.jsonBuilder().map(argsMap);
                    var json = XContentHelper.convertToJson(BytesReference.bytes(builder), false, XContentType.JSON);
                    return new FunctionCall(name, json);
                } catch (IOException e) {
                    logger.warn("Failed to parse and convert VertexAI function args to json", e);
                    return new FunctionCall(name, null);
                }
            }
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(FUNCTION_NAME_FIELD));
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), new ParseField(FUNCTION_ARGS_FIELD));
        }

        public static FunctionCall parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    private record UsageMetadata(int promptTokenCount, int candidatesTokenCount, int totalTokenCount) {}

    private static class UsageMetadataParser {
        private static final ConstructingObjectParser<UsageMetadata, Void> PARSER = new ConstructingObjectParser<>(
            USAGE_METADATA_FIELD,
            true,
            args -> {
                if (Objects.isNull(args[0]) && Objects.isNull(args[1]) && Objects.isNull(args[2])) {
                    return null;
                }
                return new UsageMetadata(
                    args[0] == null ? 0 : (int) args[0],
                    args[1] == null ? 0 : (int) args[1],
                    args[2] == null ? 0 : (int) args[2]
                );
            }
        );

        static {
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(PROMPT_TOKEN_COUNT_FIELD));
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(CANDIDATES_TOKEN_COUNT_FIELD));
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(TOTAL_TOKEN_COUNT_FIELD));
        }

        public static UsageMetadata parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }
}
