/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;
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
import software.amazon.awssdk.services.bedrockruntime.model.ToolUseBlockStart;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AmazonBedrockChatCompletionStreamingProcessorTests extends ESTestCase {
    private AmazonBedrockChatCompletionStreamingProcessor processor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ThreadPool threadPool = mock();
        when(threadPool.executor(UTILITY_THREAD_POOL_NAME)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        processor = new AmazonBedrockChatCompletionStreamingProcessor(threadPool, "model");
    }

    /**
     * We do not issue requests on subscribe because the downstream will control the pacing.
     */
    public void testOnSubscribeBeforeDownstreamDoesNotRequest() {
        var upstream = mock(Flow.Subscription.class);
        processor.onSubscribe(upstream);

        verify(upstream, never()).request(anyLong());
    }

    /**
     * If the downstream requests data before the upstream is set, when the upstream is set, we will forward the pending requests to it.
     */
    public void testOnSubscribeAfterDownstreamRequests() {
        var expectedRequestCount = randomLongBetween(1, 500);
        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> subscriber = mock();
        doAnswer(ans -> {
            Flow.Subscription sub = ans.getArgument(0);
            sub.request(expectedRequestCount);
            return null;
        }).when(subscriber).onSubscribe(any());
        processor.subscribe(subscriber);

        var upstream = mock(Flow.Subscription.class);
        processor.onSubscribe(upstream);

        verify(upstream, times(1)).request(anyLong());
    }

    public void testCancelDuplicateSubscriptions() {
        processor.onSubscribe(mock());

        var upstream = mock(Flow.Subscription.class);
        processor.onSubscribe(upstream);

        verify(upstream, times(1)).cancel();
        verifyNoMoreInteractions(upstream);
    }

    public void testMultiplePublishesCallsOnError() {
        processor.subscribe(mock());

        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> subscriber = mock();
        processor.subscribe(subscriber);

        verify(subscriber, times(1)).onError(assertArg(e -> {
            assertThat(e, isA(IllegalStateException.class));
            assertThat(e.getMessage(), equalTo("Subscriber already set."));
        }));
    }

    public void testForwardsDownstream() {
        var expectedMessageStartRole = "assistant";
        ExecutorService executorService = mock();
        ThreadPool threadPool = mock();
        when(threadPool.executor(UTILITY_THREAD_POOL_NAME)).thenReturn(executorService);
        processor = new AmazonBedrockChatCompletionStreamingProcessor(threadPool, "model");
        doAnswer(ans -> {
            Runnable command = ans.getArgument(0);
            command.run();
            return null;
        }).when(executorService).execute(any());

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);
        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        processor.subscribe(downstream);

        ConverseStreamOutput messageStartOutput = messageStartOutput(expectedMessageStartRole);
        ConverseStreamOutput contentBlockStartOutput = contentBlockStartOutput();
        ConverseStreamOutput contentBlockDeltaOutput = contentBlockDeltaOutput();
        ConverseStreamOutput contentBlockStopOutput = contentBlockStopOutput();
        ConverseStreamOutput messageStopOutput = messageStopOutput();
        ConverseStreamOutput metadata = metadataOutput();

        processor.onNext(messageStartOutput);
        processor.onNext(contentBlockStartOutput);
        processor.onNext(contentBlockDeltaOutput);

        verifyMessageStart(downstream, expectedMessageStartRole);
        verifyContentBlockStart(downstream);
        verifyContentBlockDelta(downstream);

        verify(executorService, times(1)).execute(any());
        verify(upstream, times(0)).request(anyLong());

        processor.onNext(contentBlockStopOutput);
        processor.onNext(messageStopOutput);
        processor.onNext(metadata);
        verifyContentBlockStop(downstream);
        verifyMessageStop(downstream);
        verifyMetadata(downstream);
    }

    private void verifyMessageStart(Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream, String expectedText) {
        verify(downstream, times(1)).onNext(assertArg(results -> {
            assertThat(results, notNullValue());
            assertThat(results.chunks().size(), equalTo(1));
            assertThat(results.chunks().getFirst().choices().getFirst().delta().role(), equalTo(expectedText));
        }));
    }

    private ConverseStreamOutput messageStartOutput(String role) {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.MESSAGE_START);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            MessageStartEvent event = MessageStartEvent.builder().role(role).build();
            visitor.visitMessageStart(event);
            return null;
        }).when(output).accept(any());
        return output;
    }

    private ConverseStreamOutput contentBlockStartOutput() {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.CONTENT_BLOCK_START);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            ContentBlockStartEvent event = ContentBlockStartEvent.builder()
                .start(ContentBlockStart.builder().toolUse(ToolUseBlockStart.builder().build()).build())
                .build();
            visitor.visitContentBlockStart(event);
            return null;
        }).when(output).accept(any());
        return output;
    }

    private ConverseStreamOutput contentBlockDeltaOutput() {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.CONTENT_BLOCK_DELTA);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            ContentBlockDelta delta = ContentBlockDelta.builder().build();
            ContentBlockDeltaEvent event = ContentBlockDeltaEvent.builder().delta(delta).build();
            visitor.visitContentBlockDelta(event);
            return null;
        }).when(output).accept(any());
        return output;
    }

    private ConverseStreamOutput contentBlockStopOutput() {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.CONTENT_BLOCK_STOP);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            ContentBlockStopEvent event = ContentBlockStopEvent.builder().build();
            visitor.visitContentBlockStop(event);
            return null;
        }).when(output).accept(any());
        return output;
    }

    private ConverseStreamOutput messageStopOutput() {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.MESSAGE_STOP);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            MessageStopEvent event = MessageStopEvent.builder().build();
            visitor.visitMessageStop(event);
            return null;
        }).when(output).accept(any());
        return output;
    }

    private ConverseStreamOutput metadataOutput() {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.METADATA);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            ConverseStreamMetadataEvent event = ConverseStreamMetadataEvent.builder().build();
            visitor.visitMetadata(event);
            return null;
        }).when(output).accept(any());
        return output;
    }

    private void verifyContentBlockStart(Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream) {
        verifyDownstream(downstream);
    }

    private void verifyContentBlockDelta(Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream) {
        verifyDownstream(downstream);
    }

    private void verifyContentBlockStop(Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream) {
        verifyDownstream(downstream);
    }

    private void verifyMessageStop(Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream) {
        verifyDownstream(downstream);
    }

    private void verifyMetadata(Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream) {
        verifyDownstream(downstream);
    }

    private static void verifyDownstream(Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream) {
        verify(downstream, times(1)).onNext(assertArg(results -> {
            assertThat(results, notNullValue());
            assertThat(results.chunks().size(), equalTo(1));
        }));
    }

    public void verifyCompleteBeforeRequest() {
        processor.onComplete();

        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        verify(downstream, times(1)).onComplete();
    }

    public void verifyCompleteAfterRequest() {

        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        processor.onComplete();
        verify(downstream, times(1)).onComplete();
    }

    public void verifyOnErrorBeforeRequest() {
        var expectedError = BedrockRuntimeException.builder().message("ahhhhhh").build();
        processor.onError(expectedError);

        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        verify(downstream, times(1)).onError(assertArg(e -> {
            assertThat(e, isA(ElasticsearchException.class));
            assertThat(e.getCause(), is(expectedError));
        }));
    }

    public void verifyOnErrorAfterRequest() {
        var expectedError = BedrockRuntimeException.builder().message("ahhhhhh").build();

        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        processor.onError(expectedError);
        verify(downstream, times(1)).onError(assertArg(e -> {
            assertThat(e, isA(ElasticsearchException.class));
            assertThat(e.getCause(), is(expectedError));
        }));
    }

    public void verifyAsyncOnCompleteIsStillDeliveredSynchronously() {
        mockUpstream();

        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        verify(downstream, times(1)).onNext(any());
        processor.onComplete();
        verify(downstream, times(0)).onComplete();
        sub.getValue().request(1);
        verify(downstream, times(1)).onComplete();
    }

    private void mockUpstream() {
        Flow.Subscription upstream = mock();
        doAnswer(ans -> {
            processor.onNext(messageStartOutput(randomIdentifier()));
            processor.onNext(contentBlockStartOutput());
            processor.onNext(contentBlockDeltaOutput());
            processor.onNext(contentBlockStopOutput());
            processor.onNext(messageStopOutput());
            processor.onNext(metadataOutput());
            return null;
        }).when(upstream).request(anyLong());
        processor.onSubscribe(upstream);
    }
}
