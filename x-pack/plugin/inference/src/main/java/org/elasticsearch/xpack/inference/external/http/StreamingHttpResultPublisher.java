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
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.nio.util.SimpleInputBuffer;
import org.apache.http.protocol.HttpContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/**
 * <p>Streams responses from Apache's HttpAsyncResponseConsumer to Java's Flow.Publisher.</p>
 *
 * <p>The ActionListener is called once when the HttpResponse is initially received to establish the Flow.  All subsequent responses and
 * errors will be sent through the Flow API.  Consumers are expected to call #onSubscribe with a
 * {@link java.util.concurrent.Flow.Subscriber} when the ActionListener is called.  Consumers can then begin using the resulting
 * {@link java.util.concurrent.Flow.Subscription} to request HttpResults from this publisher.</p>
 *
 * <p>Consumers should expect the first HttpResult to represent the overall HttpResponse.  Apache opens the channel with an HttpResponse
 * before it starts sending response bytes.  If the HttpResponse is an error, Apache may only send an HttpResponse with an HttpEntity,
 * so this publisher will send a single HttpResult. If the HttpResponse is healthy, Apache will send an HttpResponse with or without
 * the HttpEntity.</p>
 */
class StreamingHttpResultPublisher implements HttpAsyncResponseConsumer<HttpResponse>, Flow.Publisher<HttpResult> {
    private final HttpSettings settings;
    private final ActionListener<Flow.Publisher<HttpResult>> listener;

    // used to manage the HTTP response
    private volatile HttpResponse response;
    private volatile Exception ex;

    // used to control the state of this publisher (Apache) and its interaction with its subscriber
    private final AtomicBoolean isDone = new AtomicBoolean(false);
    private final AtomicBoolean subscriptionCanceled = new AtomicBoolean(false);
    private volatile Flow.Subscriber<? super HttpResult> subscriber;

    private final RequestBasedTaskRunner taskRunner;
    private final AtomicBoolean pendingRequest = new AtomicBoolean(false);
    private final Deque<Runnable> queue = new ConcurrentLinkedDeque<>();

    // used to control the flow of data from the Apache client, if we're producing more bytes than we can consume then we'll pause
    private final AtomicLong bytesInQueue = new AtomicLong(0);
    private final Object ioLock = new Object();
    private volatile IOControl savedIoControl;

    StreamingHttpResultPublisher(ThreadPool threadPool, HttpSettings settings, ActionListener<Flow.Publisher<HttpResult>> listener) {
        this.settings = Objects.requireNonNull(settings);
        this.listener = Objects.requireNonNull(listener);

        this.taskRunner = new RequestBasedTaskRunner(new OffloadThread(), threadPool, UTILITY_THREAD_POOL_NAME);
    }

    @Override
    public void responseReceived(HttpResponse httpResponse) throws IOException {
        this.response = httpResponse;
        if (response.getEntity() == null || response.getEntity().getContentLength() <= 0) {
            // on success, we may receive an empty content payload to initiate the stream
            this.queue.offer(() -> subscriber.onNext(new HttpResult(response, new byte[0])));
        } else {
            var firstResponse = HttpResult.create(settings.getMaxResponseSize(), response);
            this.queue.offer(() -> subscriber.onNext(firstResponse));
        }
        this.listener.onResponse(this);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super HttpResult> subscriber) {
        if (this.subscriber != null) {
            subscriber.onError(new IllegalStateException("Only one subscriber is allowed for this Publisher."));
            return;
        }

        this.subscriber = subscriber;
        subscriber.onSubscribe(new HttpSubscription());
    }

    @Override
    public void consumeContent(ContentDecoder contentDecoder, IOControl ioControl) throws IOException {
        // if the subscriber canceled us, tell Apache
        if (subscriptionCanceled.get()) {
            ioControl.shutdown();
            return;
        }

        var buffer = new SimpleInputBuffer(4096);
        var consumed = buffer.consumeContent(contentDecoder);
        var allBytes = new byte[consumed];
        buffer.read(allBytes);

        // we can have empty bytes, don't bother sending them
        if (allBytes.length > 0) {
            queue.offer(() -> {
                subscriber.onNext(new HttpResult(response, allBytes));
                var currentBytesInQueue = bytesInQueue.updateAndGet(current -> Long.max(0, current - allBytes.length));
                if (savedIoControl != null) {
                    var maxBytes = settings.getMaxResponseSize().getBytes() * 0.5;
                    if (currentBytesInQueue <= maxBytes) {
                        resumeProducer();
                    }
                }
            });
        }

        // always check if totalByteSize > the configured setting in case the settings change
        if (bytesInQueue.accumulateAndGet(allBytes.length, Long::sum) >= settings.getMaxResponseSize().getBytes()) {
            pauseProducer(ioControl);
        }

        // always run in case we're waking up from a pause and need to start a new thread
        taskRunner.requestNextRun();
    }

    private void pauseProducer(IOControl ioControl) {
        ioControl.suspendInput();
        synchronized (ioLock) {
            savedIoControl = ioControl;
        }
    }

    private void resumeProducer() {
        synchronized (ioLock) {
            if (savedIoControl != null) {
                savedIoControl.requestInput();
                savedIoControl = null;
            }
        }
    }

    @Override
    public void responseCompleted(HttpContext httpContext) {}

    // called when Apache is failing the response
    @Override
    public void failed(Exception e) {
        if (this.isDone.compareAndSet(false, true)) {
            ex = e;
            queue.offer(() -> subscriber.onError(e));
            taskRunner.requestNextRun();
        }
    }

    // called when Apache is done with the response
    @Override
    public void close() {
        if (isDone.compareAndSet(false, true)) {
            queue.offer(() -> subscriber.onComplete());
            taskRunner.requestNextRun();
        }
    }

    // called when Apache is canceling the response
    @Override
    public boolean cancel() {
        close();
        return true;
    }

    @Override
    public Exception getException() {
        return ex;
    }

    @Override
    public HttpResponse getResult() {
        return response;
    }

    @Override
    public boolean isDone() {
        return isDone.get();
    }

    private class HttpSubscription implements Flow.Subscription {
        @Override
        public void request(long n) {
            if (subscriptionCanceled.get()) {
                return;
            }

            if (n > 0) {
                pendingRequest.set(true);
                taskRunner.requestNextRun();
            } else {
                // per Subscription's spec, fail the subscriber and stop the processor
                cancel();
                subscriber.onError(new IllegalArgumentException("Subscriber requested a non-positive number " + n));
            }
        }

        @Override
        public void cancel() {
            if (subscriptionCanceled.compareAndSet(false, true)) {
                taskRunner.cancel();
            }
        }
    }

    private class OffloadThread implements Runnable {
        @Override
        public void run() {
            if (subscriptionCanceled.get()) {
                return;
            }

            if (queue.isEmpty() == false && pendingRequest.compareAndSet(true, false)) {
                var next = queue.poll();
                if (next != null) {
                    next.run();
                } else {
                    pendingRequest.set(true);
                }
            }
        }
    }
}
