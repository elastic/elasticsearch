/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.bedrockruntime.model.*;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AmazonBedrockUnifiedStreamingChatProcessorTests extends ESTestCase {
    private AmazonBedrockUnifiedStreamingChatProcessor processor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ThreadPool threadPool = mock();
        when(threadPool.executor(UTILITY_THREAD_POOL_NAME)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        processor = new AmazonBedrockUnifiedStreamingChatProcessor(threadPool);
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

    private ConverseStreamOutput output(String text) {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.CONTENT_BLOCK_DELTA);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            ContentBlockDelta delta = ContentBlockDelta.fromText(text);
            ContentBlockDeltaEvent event = ContentBlockDeltaEvent.builder().delta(delta).build();
            visitor.visitContentBlockDelta(event);
            return null;
        }).when(output).accept(any());
        return output;
    }

    private void verifyText(Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream, String expectedText) {
        verify(downstream, times(1)).onNext(assertArg(results -> {
            assertThat(results, notNullValue());
            assertThat(results.chunks().size(), equalTo(1));
//            assertThat(results.chunks().getFirst().choices().getFirst(), equalTo(expectedText));
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
            processor.onNext(output(randomIdentifier()));
            return null;
        }).when(upstream).request(anyLong());
        processor.onSubscribe(upstream);
    }
}
