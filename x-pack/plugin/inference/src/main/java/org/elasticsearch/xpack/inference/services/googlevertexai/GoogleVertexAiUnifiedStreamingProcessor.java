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
     * Checked on the request side to filter out reasoning details from other providers.
     */
    public static final String GOOGLE_VERTEX_AI_FORMAT = "google-vertex-ai-v1";

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
        /**
         * Incremented at the start of each tool call across the stream so that clients accumulating
         * tool-call deltas by index can distinguish parallel calls.
         */
        private int toolCallIndex = 0;
        /**
         * Monotonically increasing index of the current thought block. Incremented when the block ends: either
         * a {@code thoughtSignature} closes it, or a non-thought part (text or function call) interrupts it.
         * All streamed fragments of one block share the same index.
         */
        private long reasoningIndex = 0;
        /**
         * {@code true} while consecutive thought parts are still accumulating the same reasoning block;
         * {@code false} once a signature ends the block or a non-thought part interrupts it.
         */
        private boolean inThoughtBlock = false;

        public GoogleVertexAiChatCompletionChunkParser(boolean excludeReasoning) {
            this.excludeReasoning = excludeReasoning;
        }

        /**
         * Ends an open thought block so that the next reasoning detail gets a new index. Does nothing when no block
         * is open, so a run of content parts does not advance the index.
         */
        private void endThoughtBlock() {
            if (inThoughtBlock) {
                inThoughtBlock = false;
                reasoningIndex++;
            }
        }

        private static @Nullable ChatCompletionUsageResponse usageMetadataToChunk(@Nullable UsageMetadata usage) {
            if (usage == null) {
                return null;
            }
            // Gemini's candidatesTokenCount excludes thoughtsTokenCount; in the OpenAI schema
            // completion_tokens is meant to include reasoning_tokens so we add them here.
            var thoughtsTokens = usage.thoughtsTokenCount() == null ? 0 : usage.thoughtsTokenCount();
            return new ChatCompletionUsageResponse(
                usage.candidatesTokenCount() + thoughtsTokens,
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
                        // A function call ends any open thought block.
                        endThoughtBlock();
                        var fc = part.functionCall();
                        var function = new ChatCompletionToolCallResponse.Function(fc.args(), fc.name());
                        // Gemini 3 returns an id for each function call. Older models and older responses do not, in
                        // which case the name is the only stable identifier available.
                        var toolCallId = fc.id() != null ? fc.id() : fc.name();
                        toolCalls.add(new ChatCompletionToolCallResponse(toolCallIndex++, toolCallId, function, FUNCTION_TYPE));

                        if (part.thoughtSignature() != null) {
                            // Always return function-call signatures even when reasoning is excluded.
                            // Signatures are opaque state that Gemini 3 needs for multi-turn tool use;
                            // they are not user-visible reasoning content.
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
                            // A signed-but-excluded part still closes the block so the next thought gets a new index.
                            // inThoughtBlock is never set to true in this branch, so we increment directly.
                            if (part.thoughtSignature() != null) {
                                reasoningIndex++;
                            }
                            continue;
                        }
                        // All fragments of one thought block share the same index; the block ends when a signature
                        // arrives or when a non-thought part interrupts it.
                        if (part.text() != null) {
                            reasoningTextBuilder.append(part.text());
                        }
                        reasoningDetails.add(
                            new ReasoningDetail.TextReasoningDetail(
                                GOOGLE_VERTEX_AI_FORMAT,
                                null,
                                reasoningIndex,
                                part.text(),
                                part.thoughtSignature()
                            )
                        );
                        if (part.thoughtSignature() != null) {
                            inThoughtBlock = false;
                            reasoningIndex++;
                        } else {
                            inThoughtBlock = true;
                        }
                        continue;
                    }

                    // Non-thought text part — ends any open thought block.
                    endThoughtBlock();
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

    private record Candidate(@Nullable Content content, String finishReason, int index) {}

    private static class CandidateParser {
        private static final ConstructingObjectParser<Candidate, Void> PARSER = new ConstructingObjectParser<>("candidate", true, args -> {
            var content = (Content) args[0];
            var finishReason = (String) args[1];
            var index = args[2] == null ? 0 : (int) args[2];
            return new Candidate(content, finishReason, index);
        });

        static {
            // A candidate that has nothing to say carries no content, e.g. one stopped by finishReason SAFETY.
            PARSER.declareObject(
                ConstructingObjectParser.optionalConstructorArg(),
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

    private record Content(@Nullable String role, @Nullable List<Part> parts) {}

    private static class ContentParser {
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Content, Void> PARSER = new ConstructingObjectParser<>(
            CONTENT_FIELD,
            true,
            args -> new Content((String) args[0], (List<Part>) args[1])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(ROLE_FIELD));
            // Gemini sends content without parts when the output budget ran out before any part was produced, e.g. a
            // Gemini 2.5 model that spent all of max_completion_tokens thinking: {"role": "model"} with
            // finishReason MAX_TOKENS.
            PARSER.declareObjectArray(
                ConstructingObjectParser.optionalConstructorArg(),
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
