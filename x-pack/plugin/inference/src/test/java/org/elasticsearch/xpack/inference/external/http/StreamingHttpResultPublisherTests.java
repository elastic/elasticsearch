/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.HttpResponse;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamingHttpResultPublisherTests extends ESTestCase {
    private static final byte[] message = "hello".getBytes(StandardCharsets.UTF_8);
    private static final long maxBytes = message.length;
    private ThreadPool threadPool;
    private HttpSettings settings;
    private ActionListener<Flow.Publisher<HttpResult>> listener;
    private StreamingHttpResultPublisher publisher;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = mock(ThreadPool.class);
        settings = mock(HttpSettings.class);
        listener = ActionListener.noop();

        when(threadPool.executor(UTILITY_THREAD_POOL_NAME)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(settings.getMaxResponseSize()).thenReturn(ByteSizeValue.ofBytes(maxBytes));

        publisher = new StreamingHttpResultPublisher(threadPool, settings, listener);
    }

    /**
     * When we receive an http response
     * Then we call the listener
     * And we queue the initial payload
     */
    public void testFirstResponseCallsListener() throws IOException {
        var latch = new CountDownLatch(1);
        var listener = ActionListener.<Flow.Publisher<HttpResult>>wrap(
            r -> latch.countDown(),
            e -> fail("Listener onFailure should never be called.")
        );
        publisher = new StreamingHttpResultPublisher(threadPool, settings, listener);

        publisher.responseReceived(mock(HttpResponse.class));

        assertThat("Listener's onResponse should be called when we receive a response", latch.getCount(), equalTo(0L));
    }

    /**
     * This test combines 4 test since it's easier to verify the exchange of data at once.
     *
     * Given that the subscriber has not requested data
     * When we receive an http response
     * Then the publisher enqueues data
     *
     * Given that the initial http response is queued
     * When the subscriber requests data
     * Then the subscriber immediately pulls from the queue
     *
     * Given that the queue is empty
     * When the subscriber requests data
     * Then no data is sent
     *
     * Given that the subscriber has requested data
     * When the publisher enqueues data
     * Then the publisher immediately sends that data
     */
    public void testSubscriberAndPublisherExchange() throws IOException {
        var subscriber = new TestSubscriber();
        publisher.responseReceived(mock(HttpResponse.class));

        // subscribe
        publisher.subscribe(subscriber);
        assertThat("subscribe must call onSubscribe", subscriber.subscription, notNullValue());
        assertThat("onNext should only be called once we have requested data", subscriber.httpResult, nullValue());

        // request the initial http response
        subscriber.requestData();
        assertThat("onNext was called with the initial HttpResponse", subscriber.httpResult, notNullValue());
        assertTrue("HttpResponse has an empty body (because there is no HttpEntity)", subscriber.httpResult.isBodyEmpty());
        subscriber.httpResult = null; // reset test

        // subscriber requests data, publisher has not sent data yet
        subscriber.requestData();
        assertThat("onNext should only be called once we have data to process", subscriber.httpResult, nullValue());

        // publisher sends data
        publisher.consumeContent(contentDecoder(message), mock(IOControl.class));
        assertThat("onNext was called with " + new String(message, StandardCharsets.UTF_8), subscriber.httpResult.body(), equalTo(message));
    }

    /**
     * When Apache sends a non-200 HttpResponse
     * Then we enqueue the only HttpResult and close the stream
     */
    public void testNon200Response() throws IOException {
        var subscriber = new TestSubscriber();
        // Apache sends a single response and closes the consumer
        publisher.responseReceived(mock(HttpResponse.class));
        publisher.close();

        // subscriber requests data
        publisher.subscribe(subscriber);
        assertThat("subscribe must call onSubscribe", subscriber.subscription, notNullValue());
        subscriber.requestData();
        assertThat("onNext was called with the initial HttpResponse", subscriber.httpResult, notNullValue());
        assertTrue("HttpResponse has an empty body (because there is no HttpEntity)", subscriber.httpResult.isBodyEmpty());
        subscriber.requestData();
        assertTrue("Publisher has been closed", publisher.isDone());
        assertTrue("Subscriber has been completed", subscriber.completed);
    }

    /**
     * When we load too many bytes into memory
     * Then we pause the Apache IO stream
     */
    public void testPauseApache() throws IOException {
        var ioControl = mock(IOControl.class);
        publisher.responseReceived(mock(HttpResponse.class));
        when(settings.getMaxResponseSize()).thenReturn(ByteSizeValue.ofBytes(maxBytes - 1));

        publisher.consumeContent(contentDecoder(message), ioControl);

        verify(ioControl).suspendInput();
    }

    /**
     * When we empty the bytes from memory
     * Then we resume the Apache IO stream
     */
    public void testResumeApache() throws IOException {
        var subscriber = new TestSubscriber();
        publisher.responseReceived(mock(HttpResponse.class));
        publisher.subscribe(subscriber);
        subscriber.requestData();
        subscriber.httpResult = null;

        var ioControl = mock(IOControl.class);
        when(settings.getMaxResponseSize()).thenReturn(ByteSizeValue.ofBytes(maxBytes - 1));
        publisher.consumeContent(contentDecoder(message), ioControl);
        verify(ioControl).suspendInput();

        subscriber.requestData();
        verify(ioControl).requestInput();
    }

    /**
     * Given an error from Apache
     * When the subscriber requests the next set of data
     * Then the subscriber receives the error from Apache
     */
    public void testErrorBeforeRequest() {
        var subscriber = subscribe();
        var exception = new NullPointerException("test");

        publisher.failed(exception);
        assertThat("subscriber receives exception on next request", subscriber.throwable, nullValue());

        subscriber.requestData();
        assertThat("subscriber receives exception", subscriber.throwable, is(exception));
    }

    /**
     * Given the subscriber is waiting for data
     * When Apache sends an error
     * Then the subscriber immediately receives the error
     */
    public void testErrorAfterRequest() {
        var subscriber = subscribe();
        var exception = new NullPointerException("test");

        subscriber.requestData();
        publisher.failed(exception);
        assertThat("subscriber receives exception", subscriber.throwable, is(exception));
    }

    /**
     * Given the queue is being processed
     * When Apache sends an error before the subscriber asks for more data
     * Then the error will be handled the next time the subscriber requests data
     */
    public void testErrorWhileRunningBeforeRequest() throws IOException {
        var exception = new NullPointerException("test");
        var subscriber = runBefore(() -> publisher.failed(exception));

        subscriber.requestData();
        assertThat("subscriber receives exception on next request", subscriber.throwable, nullValue());

        subscriber.requestData();
        assertThat("subscriber receives exception", subscriber.throwable, is(exception));
    }

    /**
     * Given the queue is being processed
     * When Apache sends an error after the subscriber asks for more data
     * Then the error will be forwarded by the queue processor thread
     */
    public void testErrorWhileRunningAfterRequest() throws IOException {
        var exception = new NullPointerException("test");
        var subscriber = runAfter(() -> publisher.failed(exception));

        subscriber.requestData();
        assertThat("subscriber receives exception", subscriber.throwable, is(exception));
    }

    /**
     * Given Apache closed response processing
     * When the subscriber requests more data
     * Then the subscriber is marked as completed
     */
    public void testCloseBeforeRequest() {
        var subscriber = subscribe();

        publisher.close();
        assertFalse("onComplete should not be called until the subscriber requests it", subscriber.completed);

        subscriber.requestData();
        assertTrue("onComplete should now be called", subscriber.completed);
    }

    /**
     * Given the subscriber is waiting for more data
     * When Apache closes response processing
     * Then the subscriber is marked as completed
     */
    public void testCloseAfterRequest() {
        var subscriber = subscribe();

        subscriber.requestData();
        publisher.close();
        assertTrue("onComplete should be called", subscriber.completed);
    }

    /**
     * Given the queue is being processed
     * When Apache closes the publisher
     * Then the close will be handled the next time the subscriber requests data
     */
    public void testCloseWhileRunningBeforeRequest() throws IOException {
        var subscriber = runBefore(publisher::close);

        subscriber.requestData();
        assertFalse("onComplete should not be called until the subscriber requests it", subscriber.completed);

        subscriber.requestData();
        assertTrue("onComplete should now be called", subscriber.completed);
    }

    /**
     * Given the queue is being processed
     * When Apache closes the publisher after the subscriber asks for more data
     * Then the close will be handled by the queue processor thread
     */
    public void testCloseWhileRunningAfterRequest() throws IOException {
        var subscriber = runAfter(publisher::close);
        subscriber.requestData();
        assertTrue("onComplete should now be called", subscriber.completed);
    }

    /**
     * Given Apache cancels response processing
     * When the subscriber requests more data
     * Then the subscriber is marked as completed
     */
    public void testCancelBeforeRequest() {
        var subscriber = subscribe();

        publisher.cancel();
        assertFalse("onComplete should not be called until the subscriber requests it", subscriber.completed);

        subscriber.requestData();
        assertTrue("onComplete should now be called", subscriber.completed);
    }

    /**
     * Given the subscriber is waiting for more data
     * When Apache cancels response processing
     * Then the subscriber is marked as completed
     */
    public void testCancelAfterRequest() {
        var subscriber = subscribe();

        subscriber.requestData();
        publisher.cancel();
        assertTrue("onComplete should be called", subscriber.completed);
    }

    /**
     * Given the queue is being processed
     * When Apache cancels the publisher
     * Then the cancel will be handled the next time the subscriber requests data
     */
    public void testApacheCancelWhileRunningBeforeRequest() throws IOException {
        TestSubscriber subscriber = runBefore(publisher::cancel);

        subscriber.requestData();
        assertFalse("onComplete should not be called until the subscriber requests it", subscriber.completed);

        subscriber.requestData();
        assertTrue("onComplete should now be called", subscriber.completed);
    }

    /**
     * Given the queue is being processed
     * When Apache cancels the publisher after the subscriber asks for more data
     * Then the cancel will be handled by the queue processor thread
     */
    public void testApacheCancelWhileRunningAfterRequest() throws IOException {
        TestSubscriber subscriber = runAfter(publisher::cancel);

        subscriber.requestData();
        assertTrue("onComplete should now be called", subscriber.completed);
    }

    /**
     * Given the thread is an ML Utility thread
     * When a new request is processed
     * Then it should reuse that ML Utility thread
     */
    public void testReuseMlThread() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        try {
            threadPool = spy(createThreadPool(inferenceUtilityPool()));
            publisher = new StreamingHttpResultPublisher(threadPool, settings, listener);
            var subscriber = new TestSubscriber();
            publisher.responseReceived(mock(HttpResponse.class));
            publisher.subscribe(subscriber);

            CompletableFuture.runAsync(subscriber::requestData, threadPool.executor(UTILITY_THREAD_POOL_NAME)).get(5, TimeUnit.SECONDS);
            verify(threadPool, times(1)).executor(UTILITY_THREAD_POOL_NAME);
            assertThat("onNext was called with the initial HttpResponse", subscriber.httpResult, notNullValue());
            assertTrue("HttpResponse has an empty body (because there is no HttpEntity)", subscriber.httpResult.isBodyEmpty());
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Given that content is still streaming
     * When a user cancels the Subscription
     * Then the background thread should stop processing data
     */
    public void testCancelBreaksInfiniteLoop() throws Exception {
        try {
            threadPool = spy(createThreadPool(inferenceUtilityPool()));
            publisher = new StreamingHttpResultPublisher(threadPool, settings, listener);
            publisher.responseReceived(mock(HttpResponse.class));
            // create an infinitely running Subscriber
            var subscriber = new Flow.Subscriber<HttpResult>() {
                Flow.Subscription subscription;
                boolean completed = false;

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    this.subscription = subscription;
                    subscription.request(1);
                }

                @Override
                public void onNext(HttpResult item) {
                    try {
                        publisher.consumeContent(contentDecoder(message), mock(IOControl.class));
                    } catch (IOException e) {
                        fail(e, "Failed to publish content for testCancelBreaksInfiniteLoop.");
                    }
                    subscription.request(1); // run infinitely
                }

                @Override
                public void onError(Throwable throwable) {
                    fail(throwable, "onError should never be called");
                }

                @Override
                public void onComplete() {
                    completed = true;
                }
            };
            publisher.subscribe(subscriber);

            // verify the thread has started
            verify(threadPool, times(1)).executor(UTILITY_THREAD_POOL_NAME);

            subscriber.subscription.cancel();
            assertBusy(() -> assertTrue("Subscription was not canceled in 10 seconds.", subscriber.completed));
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Given the message queue is currently being processed
     * When a new message is added to the queue
     * Then a new processor thread is not started to process that message
     */
    public void testOnlyRunOneAtATime() throws IOException {
        // start with a message published
        publisher.responseReceived(mock(HttpResponse.class));
        TestSubscriber subscriber = new TestSubscriber() {
            public void onNext(HttpResult item) {
                try {
                    // publish a second message
                    publisher.consumeContent(contentDecoder(message), mock(IOControl.class));
                    super.requestData();
                    // and then exit out of the loop
                    publisher.cancel();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                super.onNext(item);
            }
        };
        publisher.subscribe(subscriber);

        verify(threadPool, times(0)).executor(UTILITY_THREAD_POOL_NAME);
        subscriber.requestData();
        verify(threadPool, times(1)).executor(UTILITY_THREAD_POOL_NAME);
    }

    private static ContentDecoder contentDecoder(byte[] message) {
        return new ContentDecoder() {
            boolean sendBytes = true;

            @Override
            public int read(ByteBuffer byteBuffer) {
                if (sendBytes) {
                    sendBytes = false;
                    byteBuffer.put(message);
                    return message.length;
                }
                return 0;
            }

            @Override
            public boolean isCompleted() {
                return true;
            }
        };
    }

    private TestSubscriber subscribe() {
        var subscriber = new TestSubscriber();
        publisher.subscribe(subscriber);
        return subscriber;
    }

    private TestSubscriber runBefore(Runnable runDuringOnNext) throws IOException {
        publisher.responseReceived(mock(HttpResponse.class));
        TestSubscriber subscriber = new TestSubscriber() {
            public void onNext(HttpResult item) {
                runDuringOnNext.run();
                super.onNext(item);
            }
        };
        publisher.subscribe(subscriber);
        return subscriber;
    }

    private TestSubscriber runAfter(Runnable runDuringOnNext) throws IOException {
        publisher.responseReceived(mock(HttpResponse.class));
        TestSubscriber subscriber = new TestSubscriber() {
            public void onNext(HttpResult item) {
                runDuringOnNext.run();
                super.requestData();
                super.onNext(item);
            }
        };
        publisher.subscribe(subscriber);
        return subscriber;
    }

    private static class TestSubscriber implements Flow.Subscriber<HttpResult> {
        private Flow.Subscription subscription;
        private HttpResult httpResult;
        private Throwable throwable;
        private boolean completed;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
        }

        @Override
        public void onNext(HttpResult item) {
            this.httpResult = item;
        }

        @Override
        public void onError(Throwable throwable) {
            this.throwable = throwable;
        }

        @Override
        public void onComplete() {
            this.completed = true;
        }

        private void requestData() {
            subscription.request(1);
        }
    }
}
