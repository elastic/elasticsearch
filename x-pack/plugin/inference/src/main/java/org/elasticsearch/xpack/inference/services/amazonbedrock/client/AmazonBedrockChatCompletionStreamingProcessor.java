/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockDelta;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockDeltaEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockStart;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockStartEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamMetadataEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamOutput;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamResponseHandler;
import software.amazon.awssdk.services.bedrockruntime.model.MessageStartEvent;
import software.amazon.awssdk.services.bedrockruntime.model.MessageStopEvent;
import software.amazon.awssdk.services.bedrockruntime.model.StopReason;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.services.amazonbedrock.translation.ChatCompletionRole;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.translation.Constants.FINISH_REASON_CONTENT_FILTER;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.translation.Constants.FINISH_REASON_LENGTH;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.translation.Constants.FINISH_REASON_STOP;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.translation.Constants.FINISH_REASON_TOOL_CALLS;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.translation.Constants.FUNCTION_TYPE;

class AmazonBedrockChatCompletionStreamingProcessor extends AmazonBedrockStreamingProcessor<StreamingUnifiedChatCompletionResults.Results> {
    private static final Logger logger = LogManager.getLogger(AmazonBedrockChatCompletionStreamingProcessor.class);

    private static final String CHAT_COMPLETION_CHUNK_OBJECT = "chat.completion.chunk";

    private final String conversationId;
    private final String modelId;

    protected AmazonBedrockChatCompletionStreamingProcessor(ThreadPool threadPool, String modelId) {
        super(threadPool);

        conversationId = Strings.format("unified-%s", UUID.randomUUID().toString());
        this.modelId = Objects.requireNonNull(modelId);
    }

    @Override
    public void onNext(ConverseStreamOutput item) {
        try {
            processItem(item);
        } catch (Exception e) {
            logger.atWarn()
                .withThrowable(e)
                .log("Failed to process item from Amazon Bedrock provider, event type: {}", item.sdkEventType());

            handleError(e);
        }
    }

    private void handleError(Exception e) {
        if (upstream != null) {
            upstream.cancel();
        }

        // Return an error in the unified chat completion format
        onError(UnifiedChatCompletionException.fromThrowable(e));
    }

    public void processItem(ConverseStreamOutput item) {
        var chunks = new ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk>(1);

        var eventType = item.sdkEventType();
        // For each event type, we perform the processing on a utility thread because the AWS SDK will likely call the runnable using a
        // thread that it maintains. So we're trying to avoid blocking that thread.
        switch (eventType) {
            case ConverseStreamOutput.EventType.MESSAGE_START -> {
                // Demand should be reset to 0 any time we will return a result downstream on another thread (aka create a chunk and call
                // downstream.onNext on another thread).
                demand.set(0);

                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder()
                        .onMessageStart(event -> runWithErrorHandling(() -> handleMessageStart(event, chunks), eventType))
                        .build()
                );
            }
            case ConverseStreamOutput.EventType.MESSAGE_STOP -> {
                demand.set(0);

                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder()
                        .onMessageStop(event -> runWithErrorHandling(() -> handleMessageStop(event, chunks), eventType))
                        .build()
                );
            }
            case ConverseStreamOutput.EventType.CONTENT_BLOCK_START -> {
                demand.set(0);

                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder()
                        .onContentBlockStart(event -> runWithErrorHandling(() -> handleContentBlockStart(event, chunks), eventType))
                        .build()
                );
            }
            case ConverseStreamOutput.EventType.CONTENT_BLOCK_DELTA -> {
                demand.set(0);

                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder()
                        .onContentBlockDelta(event -> runWithErrorHandling(() -> handleContentBlockDelta(event, chunks), eventType))
                        .build()
                );
            }
            // Intentionally not setting demand to 0 here, as we are not producing any chunks downstream
            case ConverseStreamOutput.EventType.CONTENT_BLOCK_STOP -> item.accept(
                ConverseStreamResponseHandler.Visitor.builder()
                    .onContentBlockStop(event -> runWithErrorHandling(this::requestMoreItems, eventType))
                    .build()
            );
            case ConverseStreamOutput.EventType.METADATA -> {
                demand.set(0);

                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder()
                        .onMetadata(event -> runWithErrorHandling(() -> handleMetadata(event, chunks), eventType))
                        .build()
                );
            }
            default -> {
                logger.debug("Unknown event type [{}], skipping.", eventType);
                requestMoreItems();
            }
        }
    }

    private void runWithErrorHandling(Runnable runnable, ConverseStreamOutput.EventType eventType) {
        Runnable errorHandlingRunnable = () -> {
            try {
                runnable.run();
            } catch (Exception e) {
                logger.atWarn()
                    .withThrowable(e)
                    .log("Error occurred while processing streaming response from Amazon Bedrock provider, event type: {}", eventType);

                handleError(e);
            }
        };

        runOnUtilityThreadPool(errorHandlingRunnable);
    }

    private void handleMessageStart(MessageStartEvent event, ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks) {
        var messageStart = handleMessageStart(event);
        messageStart.forEach(chunks::offer);
        callDownStreamOnNext(chunks);
    }

    private void callDownStreamOnNext(ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks) {
        if (chunks.isEmpty() == false && downstream != null) {
            downstream.onNext(new StreamingUnifiedChatCompletionResults.Results(chunks));
        } else if (upstream != null) {
            logger.debug("No chunks to send downstream, requesting more items from upstream");
            upstream.request(1);
        }
    }

    private void handleContentBlockStart(
        ContentBlockStartEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks
    ) {
        var contentBlockStart = handleContentBlockStart(event);
        contentBlockStart.forEach(chunks::offer);
        callDownStreamOnNext(chunks);
    }

    private void handleContentBlockDelta(
        ContentBlockDeltaEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks
    ) {
        var contentBlockDelta = handleContentBlockDelta(event);
        contentBlockDelta.forEach(chunks::offer);
        callDownStreamOnNext(chunks);
    }

    private void requestMoreItems() {
        if (upstream != null) {
            upstream.request(1);
        }
    }

    private void handleMessageStop(MessageStopEvent event, ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks) {
        var messageStop = handleMessageStop(event);
        messageStop.forEach(chunks::offer);
        callDownStreamOnNext(chunks);
    }

    private void handleMetadata(
        ConverseStreamMetadataEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks
    ) {
        var messageDelta = handleMetadata(event);
        messageDelta.forEach(chunks::offer);

        callDownStreamOnNext(chunks);
    }

    /**
     * Parse a MessageStartEvent into a ChatCompletionChunk stream
     * @param event the MessageStartEvent data
     * @return a stream of ChatCompletionChunk
     */
    private Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleMessageStart(MessageStartEvent event) {
        var delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, getRole(event), null);
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, 0);
        var chunk = createChatCompletionChunk(List.of(choice), null);
        return Stream.of(chunk);
    }

    private StreamingUnifiedChatCompletionResults.ChatCompletionChunk createChatCompletionChunk(
        @Nullable List<StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice> choices,
        @Nullable StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage usage
    ) {
        return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(
            conversationId,
            choices,
            modelId,
            CHAT_COMPLETION_CHUNK_OBJECT,
            usage
        );
    }

    /**
     * This performs some validation to ensure we only return valid roles.
     */
    private static String getRole(MessageStartEvent event) {
        return ChatCompletionRole.fromString(event.roleAsString()).toString();
    }

    /**
     * Parse a MessageStopEvent into a ChatCompletionChunk stream
     * @param event the MessageStopEvent data
     * @return a stream of ChatCompletionChunk
     */
    private Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleMessageStop(MessageStopEvent event) {
        var finishReason = handleFinishReason(event.stopReason());
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(
            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, null, null),
            finishReason,
            0
        );
        var chunk = createChatCompletionChunk(List.of(choice), null);
        return Stream.of(chunk);
    }

    /**
     * This ensures consistent handling of completion termination across different providers.
     * For example, both "stop_sequence" and "end_turn" from Bedrock map to the unified "stop" reason.
     * @param stopReason the stop reason
     * @return a stop reason
     */
    private String handleFinishReason(StopReason stopReason) {
        return switch (stopReason) {
            case StopReason.TOOL_USE -> FINISH_REASON_TOOL_CALLS;
            case StopReason.MAX_TOKENS -> FINISH_REASON_LENGTH;
            case StopReason.CONTENT_FILTERED, StopReason.GUARDRAIL_INTERVENED -> FINISH_REASON_CONTENT_FILTER;
            case StopReason.END_TURN, StopReason.STOP_SEQUENCE -> FINISH_REASON_STOP;
            default -> {
                logger.warn("unhandled stop reason [{}].", stopReason);
                yield FINISH_REASON_STOP;
            }
        };
    }

    /**
     * Processes a tool initialization event from Bedrock
     * This occurs when the model first decides to use a tool, providing its name and ID.
     * Parse a MessageStartEvent into a ToolCall stream
     * @param start the ContentBlockStart data
     * @return a ToolCall
     */
    private StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall handleToolUseStart(ContentBlockStart start) {
        var toolUse = start.toolUse();
        var function = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(null, toolUse.name());
        return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
            0,
            toolUse.toolUseId(),
            function,
            FUNCTION_TYPE
        );
    }

    /**
     * Processes incremental updates to a tool call
     * This typically contains the arguments that the model wants to pass to the tool.
     * Parse a ContentBlockDelta into a ToolCall stream
     * @param delta the ContentBlockDelta data
     * @return a ToolCall
     */
    private StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall handleToolUseDelta(ContentBlockDelta delta) {
        var toolUse = delta.toolUse();
        var function = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(toolUse.input(), null);
        return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(0, null, function, FUNCTION_TYPE);
    }

    /**
     * Parse a ContentBlockStartEvent into a ChatCompletionChunk stream
     * @param event the content block start data
     * @return a stream of ChatCompletionChunk
     */
    private Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleContentBlockStart(ContentBlockStartEvent event) {
        var index = event.contentBlockIndex();
        var type = event.start().type();

        if (ContentBlockStart.Type.TOOL_USE == type) {
            var toolCall = handleToolUseStart(event.start());
            var delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(
                null,
                null,
                // The model is requesting a tool be executed, so we set the role to assistant. The "tool" role is reserved for actual
                // executions of a tool.
                ChatCompletionRole.ASSISTANT.toString(),
                List.of(toolCall)
            );
            var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, index);
            var chunk = createChatCompletionChunk(List.of(choice), null);
            return Stream.of(chunk);
        }

        logger.debug("unhandled content block start type [{}].", type);
        throw new IllegalArgumentException("unhandled content block start type [" + type + "]");
    }

    /**
     * Processes incremental content updates
     * Parse a ContentBlockDeltaEvent into a ChatCompletionChunk stream
     * @param event the event data
     * @return a stream of ChatCompletionChunk
     */
    private Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleContentBlockDelta(ContentBlockDeltaEvent event) {
        var type = event.delta().type();
        var content = event.delta().text();

        var delta = switch (type) {
            case ContentBlockDelta.Type.TEXT -> new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(
                content,
                null,
                null,
                null
            );
            case ContentBlockDelta.Type.TOOL_USE -> {
                var toolCall = handleToolUseDelta(event.delta());
                yield new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(content, null, null, List.of(toolCall));
            }
            default -> {
                logger.debug("unknown content block delta type [{}].", type);
                throw new IllegalArgumentException("unknown content block delta type [" + type + "]");
            }
        };
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, event.contentBlockIndex());

        var chunk = createChatCompletionChunk(List.of(choice), null);
        return Stream.of(chunk);
    }

    /**
     * Processes usage statistics
     * Parse a ConverseStreamMetadataEvent into a ChatCompletionChunk stream
     * @param event the event data
     * @return a stream of ChatCompletionChunk
     */
    private Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleMetadata(ConverseStreamMetadataEvent event) {
        var usage = getUsage(event);
        var chunk = createChatCompletionChunk(null, usage);
        return Stream.of(chunk);
    }

    private static StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage getUsage(ConverseStreamMetadataEvent event) {
        var inputTokens = event.usage().inputTokens();
        var outputTokens = event.usage().outputTokens();
        var totalTokens = event.usage().totalTokens();

        var cacheReadTokens = Objects.requireNonNullElse(event.usage().cacheReadInputTokens(), 0);
        var cacheWriteTokens = Objects.requireNonNullElse(event.usage().cacheWriteInputTokens(), 0);

        // Calculate prompt tokens as all input tokens (bedrock input + cache read + cache write)
        var promptTokens = inputTokens + cacheReadTokens + cacheWriteTokens;

        return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(
            outputTokens,
            promptTokens,
            totalTokens,
            event.usage().cacheReadInputTokens()
        );
    }
}
