/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChoiceResponse;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionChunkResponse;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionMessageResponse;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionToolCallResponse;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionUsageResponse;
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
import java.util.stream.Stream;

import static org.elasticsearch.xpack.inference.external.response.XContentUtils.parseObjects;

public class GoogleVertexAiUnifiedStreamingProcessor extends DelegatingProcessor<
    Deque<ServerSentEvent>,
    StreamingUnifiedChatCompletionResults.Results> {

    private static final Logger logger = LogManager.getLogger(GoogleVertexAiUnifiedStreamingProcessor.class);

    private static final String CANDIDATES_FIELD = "candidates";
    private static final String CONTENT_FIELD = "content";
    private static final String ROLE_FIELD = "role";
    private static final String PARTS_FIELD = "parts";
    private static final String TEXT_FIELD = "text";
    private static final String THOUGHT_FIELD = "thought";
    private static final String THOUGHT_SIGNATURE_FIELD = "thoughtSignature";
    private static final String FINISH_REASON_FIELD = "finishReason";
    private static final String INDEX_FIELD = "index";
    private static final String USAGE_METADATA_FIELD = "usageMetadata";
    private static final String PROMPT_TOKEN_COUNT_FIELD = "promptTokenCount";
    private static final String CANDIDATES_TOKEN_COUNT_FIELD = "candidatesTokenCount";
    private static final String TOTAL_TOKEN_COUNT_FIELD = "totalTokenCount";
    private static final String THOUGHTS_TOKEN_COUNT_FIELD = "thoughtsTokenCount";
    private static final String MODEL_VERSION_FIELD = "modelVersion";
    private static final String RESPONSE_ID_FIELD = "responseId";
    private static final String FUNCTION_CALL_FIELD = "functionCall";
    private static final String FUNCTION_NAME_FIELD = "name";
    private static final String FUNCTION_ARGS_FIELD = "args";
    private static final String FUNCTION_ID_FIELD = "id";

    private static final String CHAT_COMPLETION_CHUNK = "chat.completion.chunk";
    private static final String FUNCTION_TYPE = "function";

    /**
     * Identifies reasoning details as having come from this provider, so a client knows how to echo them back.
     */
    static final String GOOGLE_VERTEX_AI_FORMAT = "google-vertex-ai-v1";

    private final BiFunction<String, Exception, Exception> errorParser;
    private final GoogleVertexAiChatCompletionChunkParser chunkParser;

    public GoogleVertexAiUnifiedStreamingProcessor(BiFunction<String, Exception, Exception> errorParser) {
        this(errorParser, false);
    }

    public GoogleVertexAiUnifiedStreamingProcessor(BiFunction<String, Exception, Exception> errorParser, boolean excludeReasoning) {
        this.errorParser = errorParser;
        this.chunkParser = new GoogleVertexAiChatCompletionChunkParser(excludeReasoning);
    }

    @Override
    protected void next(Deque<ServerSentEvent> events) throws Exception {

        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var results = new ArrayDeque<ChatCompletionChunkResponse>(events.size());

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

    Iterator<ChatCompletionChunkResponse> parse(XContentParserConfiguration parserConfig, String event) throws IOException {
        return parseObjects(parserConfig, event, p -> Stream.of(chunkParser.parseChunk(p))).iterator();
    }

    /**
     * Converts Google's {@code generateContent} chunks into unified chat completion chunks.
     * <p>
     * Stateful: one instance handles exactly one response stream. Thought summaries are numbered with a
     * monotonically increasing index that has to keep counting across the chunks of a stream, so that a client can
     * accumulate the fragments of a single reasoning block by index.
     */
    public static class GoogleVertexAiChatCompletionChunkParser {

        private final boolean excludeReasoning;
        private long reasoningIndex;

        public GoogleVertexAiChatCompletionChunkParser(boolean excludeReasoning) {
            this.excludeReasoning = excludeReasoning;
        }

        private static @Nullable ChatCompletionUsageResponse usageMetadataToChunk(@Nullable UsageMetadata usage) {
            if (usage == null) {
                return null;
            }
            return new ChatCompletionUsageResponse(
                usage.candidatesTokenCount(),
                usage.promptTokenCount(),
                usage.totalTokenCount(),
                null,
                ChatCompletionUsageResponse.CompletionTokenDetails.ofNullable(usage.thoughtsTokenCount())
            );
        }

        private ChatCompletionChoiceResponse candidateToChoice(Candidate candidate) {
            var contentTextBuilder = new StringBuilder();
            var reasoningTextBuilder = new StringBuilder();
            List<ChatCompletionToolCallResponse> toolCalls = new ArrayList<>();
            List<ReasoningDetail> reasoningDetails = new ArrayList<>();

            String role = null;

            var contentAndPartsAreNotEmpty = candidate.content() != null
                && candidate.content().parts() != null
                && candidate.content().parts().isEmpty() == false;

            if (contentAndPartsAreNotEmpty) {
                role = candidate.content().role(); // Role is at the content level
                for (Part part : candidate.content().parts()) {
                    if (part.functionCall() != null) {
                        var fc = part.functionCall();
                        var function = new ChatCompletionToolCallResponse.Function(fc.args(), fc.name());
                        // Gemini 3 returns an id for each function call. Older models and older responses do not, in
                        // which case the name is the only stable identifier available.
                        var toolCallId = fc.id() != null ? fc.id() : fc.name();
                        toolCalls.add(
                            new ChatCompletionToolCallResponse(
                                0, // No explicit index from VertexAI so we use 0
                                toolCallId,
                                function,
                                FUNCTION_TYPE
                            )
                        );

                        if (excludeReasoning == false && part.thoughtSignature() != null) {
                            // Binding the signature to the tool call id lets a subsequent turn re-attach it to the
                            // same function call part, which Gemini 3 rejects the request without.
                            reasoningDetails.add(
                                new ReasoningDetail.TextReasoningDetail(
                                    GOOGLE_VERTEX_AI_FORMAT,
                                    toolCallId,
                                    null,
                                    null,
                                    part.thoughtSignature()
                                )
                            );
                        }
                        continue;
                    }

                    if (Boolean.TRUE.equals(part.thought())) {
                        // A thought summary is reasoning rather than user-visible content, so it is kept out of the
                        // content string even when reasoning is excluded.
                        if (excludeReasoning || (part.text() == null && part.thoughtSignature() == null)) {
                            continue;
                        }
                        if (part.text() != null) {
                            reasoningTextBuilder.append(part.text());
                        }
                        reasoningDetails.add(
                            new ReasoningDetail.TextReasoningDetail(
                                GOOGLE_VERTEX_AI_FORMAT,
                                null,
                                reasoningIndex++,
                                part.text(),
                                part.thoughtSignature()
                            )
                        );
                        continue;
                    }

                    if (part.text() != null) {
                        contentTextBuilder.append(part.text());
                    }
                    if (excludeReasoning == false && part.thoughtSignature() != null) {
                        reasoningDetails.add(
                            new ReasoningDetail.TextReasoningDetail(
                                GOOGLE_VERTEX_AI_FORMAT,
                                null,
                                reasoningIndex++,
                                null,
                                part.thoughtSignature()
                            )
                        );
                    }
                }
            }

            List<ChatCompletionToolCallResponse> finalToolCalls = toolCalls.isEmpty() ? null : toolCalls;
            List<ReasoningDetail> finalReasoningDetails = reasoningDetails.isEmpty() ? null : reasoningDetails;

            var message = new ChatCompletionMessageResponse(
                contentTextBuilder.isEmpty() ? null : contentTextBuilder.toString(),
                null,
                role,
                finalToolCalls,
                reasoningTextBuilder.isEmpty() ? null : reasoningTextBuilder.toString(),
                finalReasoningDetails
            );

            return new ChatCompletionChoiceResponse(message, candidate.finishReason(), candidate.index());
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ParsedChunk, Void> PARSER = new ConstructingObjectParser<>(
            "google_vertexai_chat_completion_chunk",
            true,
            args -> new ParsedChunk((List<Candidate>) args[0], (UsageMetadata) args[1], (String) args[2], (String) args[3])
        );

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

        /**
         * Parses a single chunk in isolation. Used by the non-streaming completion path, which sees one response and
         * therefore needs no reasoning state carried between chunks.
         */
        public static ChatCompletionChunkResponse parse(XContentParser parser) throws IOException {
            return new GoogleVertexAiChatCompletionChunkParser(false).parseChunk(parser);
        }

        public ChatCompletionChunkResponse parseChunk(XContentParser parser) throws IOException {
            var parsedChunk = PARSER.parse(parser, null);
            var candidates = parsedChunk.candidates();

            var candidatesIsEmpty = candidates == null || candidates.isEmpty();
            List<ChatCompletionChoiceResponse> choices = candidatesIsEmpty
                ? Collections.emptyList()
                : candidates.stream().map(this::candidateToChoice).toList();

            return new ChatCompletionChunkResponse(
                parsedChunk.responseId(),
                choices,
                parsedChunk.modelVersion(),
                CHAT_COMPLETION_CHUNK,
                usageMetadataToChunk(parsedChunk.usage())
            );
        }
    }

    // --- Nested Parsers for Google Vertex AI structure ---

    private record ParsedChunk(List<Candidate> candidates, UsageMetadata usage, String modelVersion, String responseId) {}

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

    /**
     * {@code thought} and {@code thoughtSignature} are siblings of the part's data rather than a kind of data, so a
     * part can be a signed thought summary ({@code text} plus {@code thought}) or a signed function call.
     */
    private record Part(
        @Nullable String text,
        @Nullable FunctionCall functionCall,
        @Nullable Boolean thought,
        @Nullable String thoughtSignature
    ) {}

    private static class PartParser {
        private static final ConstructingObjectParser<Part, Void> PARSER = new ConstructingObjectParser<>(
            "part",
            true,
            args -> new Part((String) args[0], (FunctionCall) args[1], (Boolean) args[2], (String) args[3])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(TEXT_FIELD));
            PARSER.declareObject(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> FunctionCallParser.parse(p),
                new ParseField(FUNCTION_CALL_FIELD)
            );
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), new ParseField(THOUGHT_FIELD));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(THOUGHT_SIGNATURE_FIELD));
        }

        public static Part parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    private record FunctionCall(String name, String args, @Nullable String id) {}

    private static class FunctionCallParser {
        private static final ConstructingObjectParser<FunctionCall, Void> PARSER = new ConstructingObjectParser<>(
            FUNCTION_CALL_FIELD,
            true,
            args -> {
                var name = (String) args[0];
                var id = (String) args[2];

                @SuppressWarnings("unchecked")
                var argsMap = (Map<String, String>) args[1];
                if (argsMap == null) {
                    return new FunctionCall(name, null, id);
                }
                try {
                    var builder = XContentFactory.jsonBuilder().map(argsMap);
                    var json = XContentHelper.convertToJson(BytesReference.bytes(builder), false, XContentType.JSON);
                    return new FunctionCall(name, json, id);
                } catch (IOException e) {
                    logger.warn("Failed to parse and convert VertexAI function args to json", e);
                    return new FunctionCall(name, null, id);
                }
            }
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(FUNCTION_NAME_FIELD));
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), new ParseField(FUNCTION_ARGS_FIELD));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(FUNCTION_ID_FIELD));
        }

        public static FunctionCall parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    private record UsageMetadata(
        int promptTokenCount,
        int candidatesTokenCount,
        int totalTokenCount,
        @Nullable Integer thoughtsTokenCount
    ) {}

    private static class UsageMetadataParser {
        private static final ConstructingObjectParser<UsageMetadata, Void> PARSER = new ConstructingObjectParser<>(
            USAGE_METADATA_FIELD,
            true,
            args -> {
                if (Objects.isNull(args[0]) && Objects.isNull(args[1]) && Objects.isNull(args[2]) && Objects.isNull(args[3])) {
                    return null;
                }
                return new UsageMetadata(
                    args[0] == null ? 0 : (int) args[0],
                    args[1] == null ? 0 : (int) args[1],
                    args[2] == null ? 0 : (int) args[2],
                    (Integer) args[3]
                );
            }
        );

        static {
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(PROMPT_TOKEN_COUNT_FIELD));
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(CANDIDATES_TOKEN_COUNT_FIELD));
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(TOTAL_TOKEN_COUNT_FIELD));
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(THOUGHTS_TOKEN_COUNT_FIELD));
        }

        public static UsageMetadata parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }
}
