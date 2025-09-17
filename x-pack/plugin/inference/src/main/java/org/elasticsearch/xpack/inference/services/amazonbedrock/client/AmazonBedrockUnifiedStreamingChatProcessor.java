/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockDeltaEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockStartEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamMetadataEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamOutput;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamResponseHandler;
import software.amazon.awssdk.services.bedrockruntime.model.MessageStartEvent;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseFieldsValue;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

@SuppressWarnings("checkstyle:LineLength")
class AmazonBedrockUnifiedStreamingChatProcessor
    implements
        Flow.Processor<ConverseStreamOutput, StreamingUnifiedChatCompletionResults.Results> {
    private static final Logger logger = LogManager.getLogger(AmazonBedrockStreamingChatProcessor.class);
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Anthropic chat completions response";

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
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        var chunks = new ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk>(1);

        var eventType = item.sdkEventType();
        switch (eventType) {
            case ConverseStreamOutput.EventType.MESSAGE_START -> {
                demand.set(0); // reset demand before we fork to another thread
                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder()
                        .onMessageStart(event -> handleMessageStart(event, chunks, parserConfig))
                        .build()
                );
                return;
            }
            case ConverseStreamOutput.EventType.CONTENT_BLOCK_START -> {
                demand.set(0); // reset demand before we fork to another thread
                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder()
                        .onContentBlockStart(event -> handleContentBlockStart(event, chunks, parserConfig))
                        .build()
                );
                return;
            }
            case ConverseStreamOutput.EventType.CONTENT_BLOCK_DELTA -> {
                demand.set(0); // reset demand before we fork to another thread
                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder()
                        .onContentBlockDelta(event -> handleContentBlockDelta(event, chunks, parserConfig))
                        .build()
                );
                return;
            }
            case ConverseStreamOutput.EventType.METADATA -> {
                demand.set(0); // reset demand before we fork to another thread
                item.accept(
                    ConverseStreamResponseHandler.Visitor.builder().onMetadata(event -> handleMetadata(event, chunks, parserConfig)).build()
                );
                return;
            }
            case ConverseStreamOutput.EventType.MESSAGE_STOP -> {
                demand.set(0); // reset demand before we fork to another thread
                item.accept(ConverseStreamResponseHandler.Visitor.builder().onMessageStop(event -> Stream.empty()).build());
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

    private void handleMessageStart(
        MessageStartEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks,
        XContentParserConfiguration parserConfig
    ) {
        runOnUtilityThreadPool(() -> {
            var data = event.role().name();
            try {
                var messageStart = parseMessageStart(parserConfig, data);
                messageStart.forEach(chunks::offer);
            } catch (Exception e) {
                logger.warn("Failed to parse message start event from Amazon Bedrock provider: {}", data);
            }
            var results = new StreamingUnifiedChatCompletionResults.Results(chunks);
            downstream.onNext(results);
        });
    }

    private void handleContentBlockStart(
        ContentBlockStartEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks,
        XContentParserConfiguration parserConfig
    ) {
        var data = event.start().toString();
        try {
            var contentBlockStart = parseContentBlockStart(parserConfig, data);
            contentBlockStart.forEach(chunks::offer);
        } catch (Exception e) {
            logger.warn("Failed to parse block start event from Amazon Bedrock provider: {}", data);
        }
        var results = new StreamingUnifiedChatCompletionResults.Results(chunks);
        downstream.onNext(results);
    }

    private void handleContentBlockDelta(
        ContentBlockDeltaEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks,
        XContentParserConfiguration parserConfig
    ) {
        runOnUtilityThreadPool(() -> {
            var data = event.delta().toString();
            try {
                var contentBlockDelta = parseContentBlockDelta(parserConfig, data);
                contentBlockDelta.forEach(chunks::offer);
            } catch (Exception e) {
                logger.warn("Failed to parse content block delta event from Amazon Bedrock provider: {}", data);
            }
            var results = new StreamingUnifiedChatCompletionResults.Results(chunks);
            downstream.onNext(results);
        });
    }

    private void handleMetadata(
        ConverseStreamMetadataEvent event,
        ArrayDeque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> chunks,
        XContentParserConfiguration parserConfig
    ) {
        runOnUtilityThreadPool(() -> {
            var data = event.toString();
            try {
                var messageDelta = parseMessageDelta(parserConfig, data);
                messageDelta.forEach(chunks::offer);
            } catch (Exception e) {
                logger.warn("Failed to parse metadata event from Amazon Bedrock provider: {}", data);
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

    // Field names
    public static final String ID_FIELD = "id";
    public static final String MODEL_FIELD = "model";
    public static final String STOP_REASON_FIELD = "stop_reason";
    public static final String STOP_SEQUENCE_FIELD = "stop_sequence";
    public static final String TYPE_FIELD = "type";
    public static final String ROLE_FIELD = "role";
    public static final String CONTENT_FIELD = "content";

    public static final String INDEX_FIELD = "index";
    public static final String NAME_FIELD = "name";
    public static final String INPUT_TOKENS_FIELD = "input_tokens";
    public static final String OUTPUT_TOKENS_FIELD = "output_tokens";
    public static final String TEXT_FIELD = "text";
    public static final String INPUT_FIELD = "input";
    public static final String PARTIAL_JSON_FIELD = "partial_json";

    // Content block types
    public static final String TEXT_DELTA_TYPE = "text_delta";
    public static final String INPUT_JSON_DELTA_TYPE = "input_json_delta";
    public static final String TOOL_USE_TYPE = "tool_use";
    public static final String TEXT_TYPE = "text";

    /**
     * Parse a message start event into a ChatCompletionChunk stream
     * @param parserConfig the parser configuration
     * @param data the event data
     * @return a stream of ChatCompletionChunk
     * @throws IOException if parsing fails
     */
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parseMessageStart(
        XContentParserConfiguration parserConfig,
        String data
    ) throws IOException {
        System.out.println("data" + data);
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, data)) {
            var role = parseStringField(jsonParser, ROLE_FIELD);
            var id = parseStringField(jsonParser, ID_FIELD);
            var model = parseStringField(jsonParser, MODEL_FIELD);
            var finishReason = parseStringOrNullField(jsonParser, STOP_REASON_FIELD);
            var promptTokens = parseNumberField(jsonParser, INPUT_TOKENS_FIELD);
            var completionTokens = parseNumberField(jsonParser, OUTPUT_TOKENS_FIELD);
            var totalTokens = completionTokens + promptTokens;

            var usage = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(completionTokens, promptTokens, totalTokens);
            var delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, role, null);
            var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, finishReason, 0);
            var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(id, List.of(choice), model, null, usage);

            return Stream.of(chunk);
        }
    }

    /**
     * Parse a content block start event into a ChatCompletionChunk stream
     * @param parserConfig the parser configuration
     * @param data the event data
     * @return a stream of ChatCompletionChunk
     * @throws IOException if parsing fails
     */
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parseContentBlockStart(
        XContentParserConfiguration parserConfig,
        String data
    ) throws IOException {
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, data)) {
            var index = parseNumberField(jsonParser, INDEX_FIELD);
            var type = parseStringField(jsonParser, TYPE_FIELD);
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta delta;
            if (type.equals(TEXT_TYPE)) {
                var text = parseStringField(jsonParser, TEXT_FIELD);
                delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(text, null, null, null);
            } else if (type.equals(TOOL_USE_TYPE)) {
                var id = parseStringField(jsonParser, ID_FIELD);
                var name = parseStringField(jsonParser, NAME_FIELD);
                var input = parseFieldValue(jsonParser, INPUT_FIELD);
                delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(
                    null,
                    null,
                    null,
                    List.of(
                        new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
                            0,
                            id,
                            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(
                                input != null ? input.toString() : null,
                                name
                            ),
                            null
                        )
                    )
                );
            } else {
                logger.debug("Unknown content block start type [{}] for line [{}].", type, data);
                return Stream.empty();
            }
            var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, index);
            var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);
            return Stream.of(chunk);
        }
    }

    /**
     * Parse a content block delta event into a ChatCompletionChunk stream
     * @param parserConfig the parser configuration
     * @param data the event data
     * @return a stream of ChatCompletionChunk
     * @throws IOException if parsing fails
     */
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parseContentBlockDelta(
        XContentParserConfiguration parserConfig,
        String data
    ) throws IOException {
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, data)) {
            var index = parseNumberField(jsonParser, INDEX_FIELD);
            var type = parseStringField(jsonParser, TYPE_FIELD);
            StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta delta;
            if (type.equals(TEXT_DELTA_TYPE)) {
                var text = parseStringField(jsonParser, TEXT_FIELD);
                delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(text, null, null, null);
            } else if (type.equals(INPUT_JSON_DELTA_TYPE)) {
                var partialJson = parseStringField(jsonParser, PARTIAL_JSON_FIELD);
                delta = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(
                    null,
                    null,
                    null,
                    List.of(
                        new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall(
                            0,
                            null,
                            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta.ToolCall.Function(partialJson, null),
                            null
                        )
                    )
                );
            } else {
                logger.debug("Unknown content block delta type [{}] for line [{}].", type, data);
                return Stream.empty();
            }

            var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(delta, null, index);
            var chunk = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, null);

            return Stream.of(chunk);
        }
    }

    /**
     * Parse a message delta event into a ChatCompletionChunk stream
     * @param parserConfig the parser configuration
     * @param data the event data
     * @return a stream of ChatCompletionChunk
     * @throws IOException if parsing fails
     */
    public static Stream<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> parseMessageDelta(
        XContentParserConfiguration parserConfig,
        String data
    ) throws IOException {
        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, data)) {
            var finishReason = parseStringOrNullField(jsonParser, STOP_REASON_FIELD);
            var totalTokens = parseNumberField(jsonParser, OUTPUT_TOKENS_FIELD);

            var chunk = buildChatCompletionChunk(totalTokens, finishReason);

            return Stream.of(chunk);
        }
    }

    private static StreamingUnifiedChatCompletionResults.ChatCompletionChunk buildChatCompletionChunk(
        int totalTokens,
        String finishReason
    ) {
        var usage = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Usage(totalTokens, 0, totalTokens);
        var choice = new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(
            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(null, null, null, null),
            finishReason,
            0
        );
        return new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(null, List.of(choice), null, null, usage);
    }

    private static int parseNumberField(XContentParser jsonParser, String fieldName) throws IOException {
        positionParserAtTokenAfterField(jsonParser, fieldName, FAILED_TO_FIND_FIELD_TEMPLATE);
        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, jsonParser.currentToken(), jsonParser);
        return jsonParser.intValue();
    }

    private static String parseStringField(XContentParser jsonParser, String fieldName) throws IOException {
        positionParserAtTokenAfterField(jsonParser, fieldName, FAILED_TO_FIND_FIELD_TEMPLATE);
        ensureExpectedToken(XContentParser.Token.VALUE_STRING, jsonParser.currentToken(), jsonParser);
        return jsonParser.text();
    }

    private static String parseStringOrNullField(XContentParser jsonParser, String fieldName) throws IOException {
        positionParserAtTokenAfterField(jsonParser, fieldName, FAILED_TO_FIND_FIELD_TEMPLATE);
        return jsonParser.textOrNull();
    }

    private static Object parseFieldValue(XContentParser jsonParser, String fieldName) throws IOException {
        positionParserAtTokenAfterField(jsonParser, fieldName, FAILED_TO_FIND_FIELD_TEMPLATE);
        return parseFieldsValue(jsonParser);
    }
}
