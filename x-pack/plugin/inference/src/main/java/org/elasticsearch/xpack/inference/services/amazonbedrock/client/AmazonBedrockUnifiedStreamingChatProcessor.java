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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

@SuppressWarnings("checkstyle:LineLength")
class AmazonBedrockUnifiedStreamingChatProcessor
    implements
        Flow.Processor<ConverseStreamOutput, StreamingUnifiedChatCompletionResults.Results> {
    private static final Logger logger = LogManager.getLogger(AmazonBedrockStreamingChatProcessor.class);

    private final AtomicReference<Throwable> error = new AtomicReference<>(null);
    private final AtomicLong demand = new AtomicLong(0);
    private final AtomicBoolean isDone = new AtomicBoolean(false);
    private final AtomicBoolean onCompleteCalled = new AtomicBoolean(false);
    private final AtomicBoolean onErrorCalled = new AtomicBoolean(false);
    private final ThreadPool threadPool;
    private volatile Flow.Subscriber<? super StreamingUnifiedChatCompletionResults.Results> downstream;
    private volatile Flow.Subscription upstream;

    AmazonBedrockUnifiedStreamingChatProcessor(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super StreamingUnifiedChatCompletionResults.Results> subscriber) {
        if (downstream == null) {
            downstream = subscriber;
            downstream.onSubscribe(new StreamSubscription());
        } else {
            subscriber.onError(new IllegalStateException("Subscriber already set."));
        }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (upstream == null) {
            upstream = subscription;
            var currentRequestCount = demand.getAndUpdate(i -> 0);
            if (currentRequestCount > 0) {
                upstream.request(currentRequestCount);
            }
        } else {
            subscription.cancel();
        }
    }

    @Override
    public void onNext(ConverseStreamOutput item) {
        var chunks = new ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk>(1);

        var eventType = item.sdkEventType();
        switch (eventType) {
            case ConverseStreamOutput.EventType.MESSAGE_START -> {
                demand.set(0); // reset demand before we fork to another thread
                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder().onMessageStart(event -> handleMessageStart(event, chunks)).build()
                );
                return;
            }
            case ConverseStreamOutput.EventType.MESSAGE_STOP -> {
                demand.set(0); // reset demand before we fork to another thread
                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder().onMessageStop(event -> handleMessageStop(event, chunks)).build()
                );
                return;
            }
            case ConverseStreamOutput.EventType.CONTENT_BLOCK_START -> {
                demand.set(0); // reset demand before we fork to another thread
                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder()
                        .onContentBlockStart(event -> handleContentBlockStart(event, chunks))
                        .build()
                );
                return;
            }
            case ConverseStreamOutput.EventType.CONTENT_BLOCK_DELTA -> {
                demand.set(0); // reset demand before we fork to another thread
                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder()
                        .onContentBlockDelta(event -> handleContentBlockDelta(event, chunks))
                        .build()
                );
                return;
            }
            case ConverseStreamOutput.EventType.METADATA -> {
                demand.set(0); // reset demand before we fork to another thread
                item.accept(ConverseStreamResponseHandler.Visitor.builder().onMetadata(event -> handleMetadata(event, chunks)).build());
                return;
            }
            case ConverseStreamOutput.EventType.CONTENT_BLOCK_STOP -> {
                demand.set(0); // reset demand before we fork to another thread
                item.accept(ConverseStreamResponseHandler.Visitor.builder().onContentBlockStop(event -> Stream.empty()).build());
                return;
            }
            default -> {
                logger.debug("Unknown event type [{}] for line [{}].", eventType, item);
            }
        }

        if (item.sdkEventType() == ConverseStreamOutput.EventType.CONTENT_BLOCK_DELTA) {

        } else {
            upstream.request(1);
        }
    }

    private void handleMessageStart(MessageStartEvent event, ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks) {
        runOnUtilityThreadPool(() -> {
            try {
                var messageStart = handleMessageStart(event);
                messageStart.forEach(chunks::offer);
            } catch (Exception e) {
                logger.warn("Failed to parse message start event from Amazon Bedrock provider: {}", event);
            }
            if (chunks.isEmpty()) {
                upstream.request(1);
            } else {
                downstream.onNext(new StreamingUnifiedChatCompletionResults.Results(chunks));
            }
        });
    }

    private void handleMessageStop(MessageStopEvent event, ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks) {
        runOnUtilityThreadPool(() -> {
            try {
                var messageStop = handleMessageStop(event);
                messageStop.forEach(chunks::offer);
            } catch (Exception e) {
                logger.warn("Failed to parse message stop event from Amazon Bedrock provider: {}", event);
            }
            if (chunks.isEmpty()) {
                upstream.request(1);
            } else {
                downstream.onNext(new StreamingUnifiedChatCompletionResults.Results(chunks));
            }
        });
    }

    private void handleContentBlockStart(
        ContentBlockStartEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks
    ) {
        try {
            var contentBlockStart = handleContentBlockStart(event);
            contentBlockStart.forEach(chunks::offer);
        } catch (Exception e) {
            logger.warn("Failed to parse block start event from Amazon Bedrock provider: {}", event);
        }
        var results = new StreamingUnifiedChatCompletionResults.Results(chunks);
        downstream.onNext(results);
    }

    private void handleContentBlockDelta(
        ContentBlockDeltaEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks
    ) {
        runOnUtilityThreadPool(() -> {
            try {
                var contentBlockDelta = handleContentBlockDelta(event);
                contentBlockDelta.forEach(chunks::offer);
            } catch (Exception e) {
                logger.warn("Failed to parse content block delta event from Amazon Bedrock provider: {}", event);
            }
            var results = new StreamingUnifiedChatCompletionResults.Results(chunks);
            downstream.onNext(results);
        });
    }

    private void handleMetadata(
        ConverseStreamMetadataEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks
    ) {
        runOnUtilityThreadPool(() -> {
            try {
                var messageDelta = handleMetadata(event);
                messageDelta.forEach(chunks::offer);
            } catch (Exception e) {
                logger.warn("Failed to parse metadata event from Amazon Bedrock provider: {}", event);
            }
            var results = new StreamingUnifiedChatCompletionResults.Results(chunks);
            downstream.onNext(results);
        });
    }

    @Override
    public void onError(Throwable amazonBedrockRuntimeException) {
        ExceptionsHelper.maybeDieOnAnotherThread(amazonBedrockRuntimeException);
        error.set(
            new ElasticsearchException(
                Strings.format("AmazonBedrock StreamingChatProcessor failure: [%s]", amazonBedrockRuntimeException.getMessage()),
                amazonBedrockRuntimeException
            )
        );
        if (isDone.compareAndSet(false, true) && checkAndResetDemand() && onErrorCalled.compareAndSet(false, true)) {
            runOnUtilityThreadPool(() -> downstream.onError(amazonBedrockRuntimeException));
        }
    }

    private boolean checkAndResetDemand() {
        return demand.getAndUpdate(i -> 0L) > 0L;
    }

    @Override
    public void onComplete() {
        if (isDone.compareAndSet(false, true) && checkAndResetDemand() && onCompleteCalled.compareAndSet(false, true)) {
            downstream.onComplete();
        }
    }

    private void runOnUtilityThreadPool(Runnable runnable) {
        try {
            threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(runnable);
        } catch (Exception e) {
            logger.error(Strings.format("failed to fork [%s] to utility thread pool", runnable), e);
        }
    }

    private class StreamSubscription implements Flow.Subscription {
        @Override
        public void request(long n) {
            if (n > 0L) {
                demand.updateAndGet(i -> {
                    var sum = i + n;
                    return sum >= 0 ? sum : Long.MAX_VALUE;
                });
                if (upstream == null) {
                    // wait for upstream to subscribe before forwarding request
                    return;
                }
                if (upstreamIsRunning()) {
                    requestOnMlThread(n);
                } else if (error.get() != null && onErrorCalled.compareAndSet(false, true)) {
                    downstream.onError(error.get());
                } else if (onCompleteCalled.compareAndSet(false, true)) {
                    downstream.onComplete();
                }
            } else {
                cancel();
                downstream.onError(new IllegalStateException("Cannot request a negative number."));
            }
        }

        private boolean upstreamIsRunning() {
            return isDone.get() == false && error.get() == null;
        }

        private void requestOnMlThread(long n) {
            var currentThreadPool = EsExecutors.executorName(Thread.currentThread().getName());
            if (UTILITY_THREAD_POOL_NAME.equalsIgnoreCase(currentThreadPool)) {
                upstream.request(n);
            } else {
                runOnUtilityThreadPool(() -> upstream.request(n));
            }
        }

        @Override
        public void cancel() {
            if (upstream != null && upstreamIsRunning()) {
                upstream.cancel();
            }
        }
    }

    /**
     * Parse a MessageStartEvent into a ChatCompletionChunk stream
     * @param event the MessageStartEvent data
     * @return a stream of ChatCompletionChunk
     */
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleMessageStart(MessageStartEvent event) {
        var delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, event.roleAsString(), null);
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, 0);
        var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);
        return Stream.of(chunk);
    }

    /**
     * Parse a MessageStopEvent into a ChatCompletionChunk stream
     * @param event the MessageStopEvent data
     * @return a stream of ChatCompletionChunk
     */
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleMessageStop(MessageStopEvent event) {
        var finishReason = handleFinishReason(event.stopReason());
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(null, finishReason, 0);
        var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);
        return Stream.of(chunk);
    }

    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> processEvent(MessageStopEvent event) {
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
    public static String handleFinishReason(StopReason stopReason) {
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

    public StreamingUnifiedChatCompletionResults.ChatCompletionChunk createBaseChunk() {
        return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, null, null, "chat.completion.chunk", null);
    }

    /**
     * processes a tool initialization event from Bedrock
     * This occurs when the model first decides to use a tool, providing its name and ID.
     * Parse a MessageStartEvent into a ToolCall stream
     * @param start the ContentBlockStart data
     * @return a ToolCall
     */
    private static StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall handleToolUseStart(
        ContentBlockStart start
    ) {
        var type = start.type();
        var toolUse = start.toolUse();
        var function = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(null, toolUse.name());
        return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
            0,
            toolUse.toolUseId(),
            function,
            type.name()
        );
    }

    /**
     * processes incremental updates to a tool call
     * This typically contains the arguments that the model wants to pass to the tool.
     * Parse a ContentBlockDelta into a ToolCall stream
     * @param delta the ContentBlockDelta data
     * @return a ToolCall
     */
    private static StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall handleToolUseDelta(
        ContentBlockDelta delta
    ) {
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
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleContentBlockStart(ContentBlockStartEvent event) {
        var index = event.contentBlockIndex();
        var type = event.start().type();

        switch (type) {
            case ContentBlockStart.Type.TOOL_USE -> {
                var toolCall = handleToolUseStart(event.start());
                var role = "assistant";
                var delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, role, List.of(toolCall));
                var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, index);
                var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);
                return Stream.of(chunk);
            }
            default -> logger.debug("unhandled content block start type [{}].", type);
        }
        var delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, null, null);
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, index);
        var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);
        return Stream.of(chunk);
    }

    /**
     * processes incremental content updates
     * Parse a ContentBlockDeltaEvent into a ChatCompletionChunk stream
     * @param event the event data
     * @return a stream of ChatCompletionChunk
     */
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleContentBlockDelta(ContentBlockDeltaEvent event) {
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
     * processes usage statistics
     * Parse a ConverseStreamMetadataEvent into a ChatCompletionChunk stream
     * @param event the event data
     * @return a stream of ChatCompletionChunk
     */
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> handleMetadata(ConverseStreamMetadataEvent event) {
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
}
