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
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamOutput;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamResponseHandler;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.StreamingCompletionResults;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AmazonBedrockCompletionStreamingProcessorTests extends ESTestCase {

    private static final String BAD_INPUT = "bad input";

    private AmazonBedrockCompletionStreamingProcessor processor;

    @Before
    public void createProcessor() throws Exception {
        ThreadPool threadPool = mock();
        when(threadPool.executor(UTILITY_THREAD_POOL_NAME)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        processor = new AmazonBedrockCompletionStreamingProcessor(threadPool);
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
        Flow.Subscriber<StreamingCompletionResults.Results> subscriber = mock();
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

        Flow.Subscriber<StreamingCompletionResults.Results> subscriber = mock();
        processor.subscribe(subscriber);

        verify(subscriber, times(1)).onError(assertArg(e -> {
            assertThat(e, isA(IllegalStateException.class));
            assertThat(e.getMessage(), equalTo("Subscriber already set."));
        }));
    }

    public void testNonDeltaBlocksAreSkipped() {
        var upstream = mock(Flow.Subscription.class);
        processor.onSubscribe(upstream);
        var counter = new AtomicInteger();
        Arrays.stream(ConverseStreamOutput.EventType.values())
            .filter(type -> type != ConverseStreamOutput.EventType.CONTENT_BLOCK_DELTA)
            .forEach(type -> {
                ConverseStreamOutput output = mock();
                when(output.sdkEventType()).thenReturn(type);
                processor.onNext(output);
                verify(upstream, times(counter.incrementAndGet())).request(eq(1L));
            });
    }

    public void testDeltaBlockForwardsDownstream() {
        var expectedText = "hello";

        // mock executorservice so we can make sure we handle the response on another thread
        ExecutorService executorService = mock();
        ThreadPool threadPool = mock();
        when(threadPool.executor(UTILITY_THREAD_POOL_NAME)).thenReturn(executorService);
        processor = new AmazonBedrockCompletionStreamingProcessor(threadPool);
        doAnswer(ans -> {
            Runnable command = ans.getArgument(0);
            command.run();
            return null;
        }).when(executorService).execute(any());

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);
        Flow.Subscriber<StreamingCompletionResults.Results> downstream = mock();
        processor.subscribe(downstream);

        ConverseStreamOutput output = output(expectedText);

        processor.onNext(output);

        verifyText(downstream, expectedText);
        verify(executorService, times(1)).execute(any());
        verify(upstream, times(0)).request(anyLong());
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

    private void verifyText(Flow.Subscriber<StreamingCompletionResults.Results> downstream, String expectedText) {
        verify(downstream, times(1)).onNext(assertArg(results -> {
            assertThat(results, notNullValue());
            assertThat(results.results().size(), equalTo(1));
            assertThat(results.results().getFirst().delta(), equalTo(expectedText));
        }));
    }

    /**
     * The request can fail (e.g. a Bedrock ValidationException) before the upstream ever subscribes. The error must still reach the
     * downstream when it requests data, otherwise the stream hangs.
     */
    public void testOnErrorBeforeUpstreamSubscribesIsDeliveredOnRequest() {
        var expectedError = BedrockRuntimeException.builder().message("ahhhhhh").build();
        processor.onError(expectedError);

        Flow.Subscriber<StreamingCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        verify(downstream, times(1)).onError(assertArg(e -> {
            assertThat(e, isA(ElasticsearchException.class));
            assertThat(e.getCause(), is(expectedError));
        }));
    }

    /**
     * Once pending demand has been forwarded to the upstream, an error must still be delivered to the downstream.
     */
    public void testOnErrorAfterDemandForwardedUpstreamIsDelivered() {
        var expectedError = BedrockRuntimeException.builder().message("ahhhhhh").build();

        Flow.Subscriber<StreamingCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());
        sub.getValue().request(1);

        processor.onSubscribe(mock(Flow.Subscription.class));
        processor.onError(expectedError);

        verify(downstream, times(1)).onError(assertArg(e -> {
            assertThat(e, isA(ElasticsearchException.class));
            assertThat(e.getCause(), is(expectedError));
        }));
    }

    /**
     * A stored error is replayed right after {@link Flow.Subscriber#onSubscribe} returns when the subscriber requested data from within
     * onSubscribe (as the REST streaming subscriber does), and never before it.
     */
    public void testStoredErrorIsReplayedAfterOnSubscribeWhenRequestedInOnSubscribe() {
        var expectedError = BedrockRuntimeException.builder().message("ahhhhhh").build();
        processor.onError(expectedError);

        Flow.Subscriber<StreamingCompletionResults.Results> downstream = mock();
        doAnswer(ans -> {
            Flow.Subscription subscription = ans.getArgument(0);
            subscription.request(1);
            return null;
        }).when(downstream).onSubscribe(any());
        processor.subscribe(downstream);

        var inOrder = inOrder(downstream);
        inOrder.verify(downstream).onSubscribe(any());
        inOrder.verify(downstream).onError(assertArg(e -> assertThat(e.getCause(), is(expectedError))));
    }

    /**
     * Downstream subscribers only expect signals in response to a request, so an error that arrives while nothing has been requested
     * is parked and delivered by the next request.
     */
    public void testStoredErrorIsParkedUntilDownstreamRequests() {
        var expectedError = BedrockRuntimeException.builder().message("ahhhhhh").build();
        processor.onError(expectedError);

        Flow.Subscriber<StreamingCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());
        verify(downstream, never()).onError(any());

        sub.getValue().request(1);
        verify(downstream, times(1)).onError(assertArg(e -> assertThat(e.getCause(), is(expectedError))));
    }

    /**
     * Once the downstream's request has been answered with an item it has no outstanding request, so an error is parked until the next
     * one instead of being signalled while, for example, the previous item is still being written out.
     */
    public void testErrorAfterRequestWasAnsweredIsParkedUntilNextRequest() {
        var expectedError = BedrockRuntimeException.builder().message("ahhhhhh").build();
        processor.onSubscribe(mock(Flow.Subscription.class));

        Flow.Subscriber<StreamingCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        processor.emitNext(new StreamingCompletionResults.Results(new ArrayDeque<>()));
        verify(downstream, times(1)).onNext(any());

        processor.onError(expectedError);
        verify(downstream, never()).onError(any());

        sub.getValue().request(1);
        verify(downstream, times(1)).onError(assertArg(e -> assertThat(e.getCause(), is(expectedError))));
    }

    /**
     * An error whose delivery is already queued on the utility thread must not be turned into a completion by a request that arrives
     * before the queued delivery runs.
     */
    public void testRequestWhileErrorDeliveryIsQueuedDoesNotComplete() {
        var expectedError = BedrockRuntimeException.builder().message("ahhhhhh").build();
        var queued = new ArrayList<Runnable>();
        ExecutorService executorService = mock();
        doAnswer(ans -> {
            queued.add(ans.getArgument(0));
            return null;
        }).when(executorService).execute(any());
        ThreadPool threadPool = mock();
        when(threadPool.executor(UTILITY_THREAD_POOL_NAME)).thenReturn(executorService);
        processor = new AmazonBedrockCompletionStreamingProcessor(threadPool);

        Flow.Subscriber<StreamingCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());
        sub.getValue().request(1);
        processor.onSubscribe(mock(Flow.Subscription.class));

        processor.onError(expectedError);
        assertThat(queued.size(), is(1));

        sub.getValue().request(1);
        verify(downstream, never()).onComplete();

        queued.forEach(Runnable::run);
        verify(downstream, times(1)).onError(assertArg(e -> assertThat(e.getCause(), is(expectedError))));
        verify(downstream, never()).onComplete();
    }

    public void testOnErrorAfterCancelIsNotDelivered() {
        Flow.Subscriber<StreamingCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());
        sub.getValue().request(1);

        sub.getValue().cancel();
        processor.onError(BedrockRuntimeException.builder().message("ahhhhhh").build());

        verify(downstream, never()).onError(any());
    }

    public void verifyCompleteBeforeRequest() {
        processor.onComplete();

        Flow.Subscriber<StreamingCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        verify(downstream, times(1)).onComplete();
    }

    public void verifyCompleteAfterRequest() {

        Flow.Subscriber<StreamingCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        processor.onComplete();
        verify(downstream, times(1)).onComplete();
    }

    public void testOnErrorBeforeRequest_PropagatesBedrockStatus() {
        var bedrockError = BedrockRuntimeException.builder().message(BAD_INPUT).statusCode(400).build();
        processor.onSubscribe(mock(Flow.Subscription.class));
        processor.onError(new CompletionException(bedrockError));

        Flow.Subscriber<StreamingCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        verify(downstream, times(1)).onError(assertArg(e -> {
            assertThat(e, isA(ElasticsearchStatusException.class));
            assertThat(((ElasticsearchStatusException) e).status(), is(RestStatus.BAD_REQUEST));
            assertThat(e.getCause(), is(bedrockError));
        }));
    }

    public void testOnErrorAfterRequest_PropagatesBedrockStatus() {
        var bedrockError = BedrockRuntimeException.builder().message("bad input").statusCode(400).build();

        Flow.Subscriber<StreamingCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        processor.onError(new CompletionException(bedrockError));
        verify(downstream, times(1)).onError(assertArg(e -> {
            assertThat(e, isA(ElasticsearchStatusException.class));
            assertThat(((ElasticsearchStatusException) e).status(), is(RestStatus.BAD_REQUEST));
            assertThat(e.getCause(), is(bedrockError));
        }));
    }

    public void verifyAsyncOnCompleteIsStillDeliveredSynchronously() {
        mockUpstream();

        Flow.Subscriber<StreamingCompletionResults.Results> downstream = mock();
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
