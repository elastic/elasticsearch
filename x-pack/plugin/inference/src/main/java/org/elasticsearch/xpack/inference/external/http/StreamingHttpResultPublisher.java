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
import org.elasticsearch.common.util.concurrent.EsExecutors;
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
    private final ThreadPool threadPool;
    private final HttpSettings settings;
    private final ActionListener<Flow.Publisher<HttpResult>> listener;

    // used to manage the HTTP response
    private volatile HttpResponse response;
    private volatile Exception ex;

    // used to control the state of this publisher (Apache) and its interaction with its subscriber
    private final Deque<HttpResult> resultQueue = new ConcurrentLinkedDeque<>();
    private final AtomicBoolean pendingRequest = new AtomicBoolean(false);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isDone = new AtomicBoolean(false);
    private volatile Flow.Subscriber<? super HttpResult> subscriber;

    // used to control the flow of data from the Apache client, if we're producing more bytes than we can consume then we'll pause
    private final AtomicLong totalByteSize = new AtomicLong(0);
    private final Object ioLock = new Object();
    private volatile IOControl savedIoControl;

    StreamingHttpResultPublisher(ThreadPool threadPool, HttpSettings settings, ActionListener<Flow.Publisher<HttpResult>> listener) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.settings = Objects.requireNonNull(settings);
        this.listener = Objects.requireNonNull(listener);
    }

    @Override
    public void responseReceived(HttpResponse httpResponse) throws IOException {
        this.response = httpResponse;
        this.resultQueue.offer(HttpResult.create(settings.getMaxResponseSize(), response));
        this.listener.onResponse(this);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super HttpResult> subscriber) {
        this.subscriber = subscriber;
        subscriber.onSubscribe(new HttpSubscription());
    }

    @Override
    public void consumeContent(ContentDecoder contentDecoder, IOControl ioControl) throws IOException {
        if (isDone() == false) { // if the subscriber canceled us, don't bother to keep reading
            var buffer = new SimpleInputBuffer(4096);
            var consumed = buffer.consumeContent(contentDecoder);
            var allBytes = new byte[consumed];
            buffer.read(allBytes);

            // we can have empty bytes, don't bother sending them
            if (allBytes.length > 0) {
                resultQueue.offer(new HttpResult(response, allBytes));
            }

            // always check if totalByteSize > the configured setting in case the settings change
            if (totalByteSize.accumulateAndGet(allBytes.length, Long::sum) >= settings.getMaxResponseSize().getBytes()) {
                pauseProducer(ioControl);
            }

            // always run in case we're waking up from a pause and need to start a new thread
            runOrScheduleNextMessage();
        }
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

    // if there is currently a thread running in a loop, it should pick up this new request
    // if not, check if this thread is one of ours and reuse it
    // else, offload to a new thread so we do not block Apache's connection thread or ElasticSearch's transport thread
    private void runOrScheduleNextMessage() {
        if (subscriber != null && isRunning.compareAndSet(false, true)) {
            var currentThreadPool = EsExecutors.executorName(Thread.currentThread().getName());
            if (UTILITY_THREAD_POOL_NAME.equalsIgnoreCase(currentThreadPool)) {
                new OffloadThread().run();
            } else {
                threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(new OffloadThread());
            }
        }
    }

    @Override
    public void responseCompleted(HttpContext httpContext) {}

    @Override
    public void failed(Exception e) {
        if (this.isDone.compareAndSet(false, true)) {
            this.ex = e;
        }
        if (isRunning.get() == false && subscriber != null && pendingRequest.compareAndSet(true, false)) {
            subscriber.onError(ex);
        }
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

    @Override
    public void close() {
        // if isRunning == true, the OffloadThread will close out the subscription
        // if pendingRequest is false, the next call to request() will close out the subscription
        if (isDone.compareAndSet(false, true)
            && isRunning.get() == false
            && subscriber != null
            && pendingRequest.compareAndSet(true, false)) {
            subscriber.onComplete();
        }
    }

    @Override
    public boolean cancel() {
        resultQueue.clear();
        close();
        return true;
    }

    private class HttpSubscription implements Flow.Subscription {
        @Override
        public void request(long n) {
            pendingRequest.set(true);
            // always run in case we're waking up from a pause and need to start a new thread
            runOrScheduleNextMessage();
        }

        @Override
        public void cancel() {
            StreamingHttpResultPublisher.this.cancel();
        }
    }

    private class OffloadThread implements Runnable {
        @Override
        public void run() {
            while (resultQueue.isEmpty() == false && pendingRequest.compareAndSet(true, false)) {
                var nextRequest = resultQueue.poll();
                subscriber.onNext(nextRequest);
            }

            isRunning.set(false);

            if (isDone() && pendingRequest.compareAndSet(true, false)) {
                if (ex == null) {
                    subscriber.onComplete();
                } else {
                    subscriber.onError(ex);
                }
            } else if (savedIoControl != null) {
                resumeProducer();
            }
        }
    }
}
