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
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockStopEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamMetadataEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamOutput;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamResponseHandler;
import software.amazon.awssdk.services.bedrockruntime.model.MessageStartEvent;
import software.amazon.awssdk.services.bedrockruntime.model.MessageStopEvent;
import software.amazon.awssdk.services.bedrockruntime.model.StopReason;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockRole;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

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
            // TODO move this to the runOnUtilityThreadPool method
            logger.atWarn()
                .withThrowable(e)
                .log("Failed to process ConverseStreamOutput item from Amazon Bedrock provider, event type: {}", item.sdkEventType());

            if (upstream != null) {
                upstream.cancel();
            }
            // Return an error in the unified chat completion format
            onError(UnifiedChatCompletionException.fromThrowable(e));
        }
    }

    // TODO finish the response translation to openai format
    public void processItem(ConverseStreamOutput item) {
        var chunks = new ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk>(1);

        var eventType = item.sdkEventType();
        switch (eventType) {
            case ConverseStreamOutput.EventType.MESSAGE_START -> item.accept(
                ConverseStreamResponseHandler.Visitor.builder()
                    .onMessageStart(event -> runOnUtilityThreadPool(() -> handleMessageStart(event, chunks)))
                    .build()
            );
            case ConverseStreamOutput.EventType.CONTENT_BLOCK_START -> item.accept(
                ConverseStreamResponseHandler.Visitor.builder()
                    .onContentBlockStart(event -> runOnUtilityThreadPool(() -> handleContentBlockStart(event, chunks)))
                    .build()
            );
            case ConverseStreamOutput.EventType.CONTENT_BLOCK_DELTA -> {
                demand.set(0); // reset demand before we fork to another thread
                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder()
                        .onContentBlockDelta(event -> runOnUtilityThreadPool(() -> handleContentBlockDelta(event, chunks)))
                        .build()
                );
            }
            case ConverseStreamOutput.EventType.CONTENT_BLOCK_STOP -> item.accept(
                ConverseStreamResponseHandler.Visitor.builder()
                    .onContentBlockStop(event -> runOnUtilityThreadPool(() -> handleContentBlockStop(event, chunks)))
                    .build()
            );
            case ConverseStreamOutput.EventType.MESSAGE_STOP -> item.accept(
                ConverseStreamResponseHandler.Visitor.builder()
                    .onMessageStop(event -> runOnUtilityThreadPool(() -> handleMessageStop(event, chunks)))
                    .build()
            );
            case ConverseStreamOutput.EventType.METADATA -> item.accept(
                ConverseStreamResponseHandler.Visitor.builder()
                    .onMetadata(event -> runOnUtilityThreadPool(() -> handleMetadata(event, chunks)))
                    .build()
            );

            default -> logger.debug("Unknown event type [{}] for line [{}].", eventType, item);
        }
    }

    private void handleMessageStart(MessageStartEvent event, ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks) {
        try {
            var messageStart = handleMessageStart(event);
            messageStart.forEach(chunks::offer);
        } catch (Exception e) {
            logger.warn("Failed to parse message start event from Amazon Bedrock provider");
            throw e;
        }
        callDownStreamOnNext(chunks);
    }

    private void callDownStreamOnNext(ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks) {
        if (chunks.isEmpty() == false && downstream != null) {
            downstream.onNext(new StreamingUnifiedChatCompletionResults.Results(chunks));
        }
    }

    private void handleContentBlockStart(
        ContentBlockStartEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks
    ) {
        try {
            var contentBlockStart = handleContentBlockStart(event);
            contentBlockStart.forEach(chunks::offer);
        } catch (Exception e) {
            logger.warn("Failed to parse block start event from Amazon Bedrock provider");
            throw e;
        }
        callDownStreamOnNext(chunks);
    }

    private void handleContentBlockDelta(
        ContentBlockDeltaEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks
    ) {
        try {
            var contentBlockDelta = handleContentBlockDelta(event);
            contentBlockDelta.forEach(chunks::offer);
            callDownStreamOnNext(chunks);
        } catch (Exception e) {
            logger.warn("Failed to parse content block delta event from Amazon Bedrock provider");
            throw e;
        }
    }

    private void handleContentBlockStop(
        ContentBlockStopEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks
    ) {
        try {
            var messageDelta = handleContentBlockStop(event);
            messageDelta.forEach(chunks::offer);

            if (upstream != null) {
                upstream.request(1);
            }
        } catch (Exception e) {
            logger.warn("Failed to parse metadata event from Amazon Bedrock provider");
            throw e;
        }
    }

    private void handleMessageStop(MessageStopEvent event, ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks) {
        try {
            var messageStop = handleMessageStop(event);
            messageStop.forEach(chunks::offer);
            if (upstream != null) {
                upstream.request(1);
            } else {
                isDone.set(true);
            }
        } catch (Exception e) {
            logger.warn("Failed to parse message stop event from Amazon Bedrock provider");
            throw e;
        }
    }

    private void handleMetadata(
        ConverseStreamMetadataEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks
    ) {
        if (isDone.get()) {
            return;
        }
        try {
            var messageDelta = handleMetadata(event);
            messageDelta.forEach(chunks::offer);
        } catch (Exception e) {
            logger.warn("Failed to parse metadata event from Amazon Bedrock provider");
            throw e;
        }

        if (chunks.isEmpty() == false && downstream != null && demand.get() > 0 && isDone.get() == false) {
            long prev = demand.getAndUpdate(d -> {
                if (d == Long.MAX_VALUE) {
                    return d;
                }
                return d > 0 ? d - 1 : d;
            });

            if (prev > 0) {
                downstream.onNext(new StreamingUnifiedChatCompletionResults.Results(chunks));
            }

        }
        if (upstream != null) {
            upstream.request(1);
        }
    }

    /**
     * Parse a MessageStartEvent into a ChatCompletionChunk stream
     * @param event the MessageStartEvent data
     * @return a stream of ChatCompletionChunk
     */
    private Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleMessageStart(MessageStartEvent event) {
        var delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, getRole(event), null);
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, 0);
        var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(
            conversationId,
            List.of(choice),
            modelId,
            CHAT_COMPLETION_CHUNK_OBJECT,
            null
        );
        return Stream.of(chunk);
    }

    private static String getRole(MessageStartEvent event) {
        return AmazonBedrockRole.fromString(event.roleAsString()).toString();
    }

    /**
     * Parse a MessageStopEvent into a ChatCompletionChunk stream
     * @param event the MessageStopEvent data
     * @return a stream of ChatCompletionChunk
     */
    private Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleMessageStop(MessageStopEvent event) {
        var finishReason = handleFinishReason(event.stopReason());
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(null, finishReason, 0);
        var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);
        return Stream.of(chunk);
    }

    /**
     * This ensures consistent handling of completion termination across different providers.
     * For example, both "stop_sequence" and "end_turn" from Bedrock map to the unified "stop" reason.
     * @param stopReason the stop reason
     * @return a stop reason
     */
    private String handleFinishReason(StopReason stopReason) {
        switch (stopReason) {
            case StopReason.TOOL_USE -> {
                return "FinishReasonToolCalls";
            }
            case StopReason.MAX_TOKENS -> {
                return "FinishReasonLength";
            }
            case StopReason.CONTENT_FILTERED, StopReason.GUARDRAIL_INTERVENED -> {
                return "FinishReasonContentFilter";
            }
            case StopReason.END_TURN, StopReason.STOP_SEQUENCE -> {
                return "FinishReasonStop";
            }
            default -> {
                logger.debug("unhandled stop reason [{}].", stopReason);
                return "FinishReasonStop";
            }
        }
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
            toolUse.name()
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
        var type = delta.type();
        var toolUse = delta.toolUse();
        var function = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(toolUse.input(), null);
        return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(0, null, function, type.name());
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
                AmazonBedrockRole.ASSISTANT.toString(),
                List.of(toolCall)
            );
            var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, index);
            var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);
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
        StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta delta = null;
        var type = event.delta().type();
        var content = event.delta().text();

        switch (type) {
            case ContentBlockDelta.Type.TEXT -> {
                delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(content, null, null, null);
            }
            case ContentBlockDelta.Type.TOOL_USE -> {
                var toolCall = handleToolUseDelta(event.delta());
                delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(content, null, null, List.of(toolCall));
            }
            default -> logger.debug("unknown content block delta type [{}].", type);
        }
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, event.contentBlockIndex());
        var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);
        return Stream.of(chunk);
    }

    /**
     * Processes usage statistics
     * Parse a ConverseStreamMetadataEvent into a ChatCompletionChunk stream
     * @param event the event data
     * @return a stream of ChatCompletionChunk
     */
    private Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleMetadata(ConverseStreamMetadataEvent event) {
        var inputTokens = event.usage().inputTokens();
        var outputTokens = event.usage().outputTokens();
        var totalTokens = event.usage().totalTokens();
        var usage = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(outputTokens, inputTokens, totalTokens);
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(
            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, null, null),
            null,
            0
        );
        var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, usage);
        return Stream.of(chunk);
    }

    /**
     * Processes usage statistics
     * Parse a ContentBlockStopEvent into a ChatCompletionChunk stream
     * @param event the event data
     * @return a stream of ChatCompletionChunk
     */
    private Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleContentBlockStop(ContentBlockStopEvent event) {
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(null, null, event.contentBlockIndex());
        var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);
        return Stream.of(chunk);
    }
}
