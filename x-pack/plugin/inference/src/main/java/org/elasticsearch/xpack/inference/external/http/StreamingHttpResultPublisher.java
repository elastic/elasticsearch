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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.core.Strings.format;
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
class StreamingHttpResultPublisher implements HttpAsyncResponseConsumer<Void> {
    private static final Logger logger = LogManager.getLogger(StreamingHttpResultPublisher.class);

    private final ActionListener<StreamingHttpResult> listener;
    private final AtomicBoolean listenerCalled = new AtomicBoolean(false);

    private final AtomicBoolean isDone = new AtomicBoolean(false);
    private final AtomicBoolean subscriptionCanceled = new AtomicBoolean(false);

    private final SimpleInputBuffer inputBuffer = new SimpleInputBuffer(4096);
    private final DataPublisher publisher;
    private final ApacheClientBackpressure backpressure;
    private final String inferenceEntityId;
    private final HttpSettings settings;

    private volatile Exception exception;

    StreamingHttpResultPublisher(
        ThreadPool threadPool,
        HttpSettings settings,
        ActionListener<StreamingHttpResult> listener,
        CircuitBreaker circuitBreaker,
        String inferenceEntityId
    ) {
        this.inferenceEntityId = inferenceEntityId;
        this.settings = settings;

        this.listener = ActionListener.notifyOnce(Objects.requireNonNull(listener));

        this.publisher = new DataPublisher(threadPool);
        this.backpressure = new ApacheClientBackpressure(
            Objects.requireNonNull(settings),
            Objects.requireNonNull(circuitBreaker),
            Objects.requireNonNull(inferenceEntityId)
        );

    }

    @Override
    public void responseReceived(HttpResponse httpResponse) {
        if (listenerCalled.compareAndSet(false, true)) {
            listener.onResponse(new StreamingHttpResult(httpResponse, publisher));
        }
    }

    @Override
    public void consumeContent(ContentDecoder contentDecoder, IOControl ioControl) throws IOException {
        // if the subscriber canceled us, tell Apache
        if (subscriptionCanceled.get()) {
            ioControl.shutdown();
            return;
        }

        try {
            var consumed = inputBuffer.consumeContent(contentDecoder);
            // we could have read 0 bytes if the body was delayed getting in, we need to return out so apache can load the body/footer
            if (consumed > 0) {
                var allBytes = new byte[consumed];
                inputBuffer.read(allBytes);
                var paused = backpressure.addBytesAndMaybePause(consumed, ioControl);
                // cancelUpstream() may have raced past the first subscriptionCanceled guard, so we've to recheck
                if (subscriptionCanceled.get()) {
                    backpressure.subtractBytesAndMaybeUnpause(consumed);
                    return;
                }

                if (paused && publisher.hasSubscriber() == false) {
                    var abandoned = new IllegalStateException(
                        format(
                            "Stream for inference id [%s] buffered [%s] without a consumer, aborting",
                            inferenceEntityId,
                            settings.getMaxResponseSize()
                        )
                    );
                    exception = abandoned;
                    publisher.onError(abandoned);
                    return;
                }

                publisher.onNext(allBytes);
            }
        } catch (Exception e) {
            // if the provider closes the connection in the middle of the stream,
            // the contentDecoder will throw an exception trying to read the payload,
            // we should catch that and forward it downstream so we can properly handle it
            exception = e;
            publisher.onError(e);
        } finally {
            inputBuffer.reset();
        }
    }

    @Override
    public void responseCompleted(HttpContext httpContext) {}

    // called when Apache is failing the response
    @Override
    public void failed(Exception e) {
        if (this.isDone.compareAndSet(false, true)) {
            if (listenerCalled.compareAndSet(false, true)) {
                listener.onFailure(e);
            } else {
                exception = e;
                publisher.onError(e);
            }
        }
    }

    // called when Apache is done with the response
    @Override
    public void close() {
        if (isDone.compareAndSet(false, true)) {
            publisher.onComplete();
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
        return exception;
    }

    @Override
    public Void getResult() {
        return null;
    }

    @Override
    public boolean isDone() {
        return isDone.get();
    }

    /**
     * We only want to push payload data when the client is ready to receive it, so the client will use
     * {@link Flow.Subscription#request(long)} to request more data. We collect the payload bytes in a queue and process them on a
     * separate thread from both the Apache IO thread reading from the provider and the client's transport thread requesting more data.
     * Clients use {@link Flow.Subscription#cancel()} to exit early, and we'll forward that cancellation to the provider.
     */
    private class DataPublisher implements Flow.Processor<byte[], byte[]> {
        private final RequestBasedTaskRunner taskRunner;
        private final Deque<byte[]> contentQueue = new ConcurrentLinkedDeque<>();
        private final AtomicLong pendingRequests = new AtomicLong(0);
        private volatile Exception pendingError = null;
        private volatile boolean completed = false;
        private volatile Flow.Subscriber<? super byte[]> downstream;

        private DataPublisher(ThreadPool threadPool) {
            this.taskRunner = new RequestBasedTaskRunner(this::sendToSubscriber, threadPool, UTILITY_THREAD_POOL_NAME);
        }

        private void sendToSubscriber() {
            if (downstream == null) {
                if (pendingError != null) {
                    stopUpstream();
                } else if (completed) {
                    backpressure.releaseTrackedBytes();
                }
                return;
            }
            if (pendingRequests.get() > 0 && pendingError != null) {
                pendingRequests.decrementAndGet();
                cancelUpstream();
                downstream.onError(pendingError);
                return;
            }
            byte[] nextBytes;
            while (pendingRequests.get() > 0 && (nextBytes = contentQueue.poll()) != null) {
                pendingRequests.decrementAndGet();
                backpressure.subtractBytesAndMaybeUnpause(nextBytes.length);
                downstream.onNext(nextBytes);
            }
            /*
             * Read `completed` before inspecting the queue. If we check the queue first it could cause a bug where we let
             * Apache enqueue a chunk and complete the stream in between the two reads, which would complete the subscriber while its
             * data is still queued. This causes the response body to be truncated. `completed` is only set after Apache has handed
             * us every chunk (consumeContent and close run on the same connection-bound I/O dispatcher and are never
             * concurrent), so reading the volatile `completed == true` first establishes happens-before with every
             * prior contentQueue.offer(), making the subsequent isEmpty() check reliable.
             * See https://github.com/elastic/elasticsearch/issues/157575.
             */
            if (pendingRequests.get() > 0 && completed && contentQueue.isEmpty()) {
                pendingRequests.decrementAndGet();
                downstream.onComplete();
            }
        }

        private boolean hasSubscriber() {
            return downstream != null;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
            if (this.downstream != null) {
                subscriber.onError(new IllegalStateException("Only one subscriber is allowed for this Publisher."));
                return;
            }

            this.downstream = subscriber;
            downstream.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    if (n > 0) {
                        pendingRequests.addAndGet(n);
                        taskRunner.requestNextRun();
                    } else {
                        // per Subscription's spec, fail the subscriber and stop the processor
                        cancel();
                        subscriber.onError(new IllegalArgumentException("Subscriber requested a non-positive number " + n));
                    }
                }

                @Override
                public void cancel() {
                    cancelUpstream();
                }
            });
        }

        private void stopUpstream() {
            if (subscriptionCanceled.compareAndSet(false, true)) {
                backpressure.releaseTrackedBytes();
                backpressure.shutdownProducer();
            }
        }

        private void cancelUpstream() {
            stopUpstream();
            taskRunner.cancel();
        }

        @Override
        public void onNext(byte[] item) {
            contentQueue.offer(item);
            taskRunner.requestNextRun();
        }

        @Override
        public void onError(Throwable throwable) {
            if (throwable instanceof Exception e) {
                pendingError = e;
            } else {
                ExceptionsHelper.maybeError(throwable).ifPresent(ExceptionsHelper::maybeDieOnAnotherThread);
                pendingError = new RuntimeException("Unhandled error while streaming");
            }
            taskRunner.requestNextRun();
        }

        @Override
        public void onComplete() {
            completed = true;
            taskRunner.requestNextRun();
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            assert false : "Apache never calls this.";
            throw new UnsupportedOperationException("Apache never calls this.");
        }
    }

    /**
     * We want to keep track of how much memory we are consuming while reading the payload from the external provider. Apache continuously
     * pushes payload data to us, whereas the client only requests the next set of bytes when they are ready, so we want to track how much
     * data we are holding in memory and potentially pause the Apache client if we have reached our limit.
     */
    private static class ApacheClientBackpressure {
        private final HttpSettings settings;
        private final AtomicLong bytesInQueue = new AtomicLong(0);
        private final Object ioLock = new Object();
        private volatile IOControl savedIoControl;
        private final CircuitBreaker circuitBreaker;
        private final String inferenceEntityId;
        // true once shutdownProducer() is called
        private volatile boolean shutdown = false;

        private ApacheClientBackpressure(HttpSettings settings, CircuitBreaker circuitBreaker, String inferenceEntityId) {
            this.settings = settings;
            this.circuitBreaker = circuitBreaker;
            this.inferenceEntityId = inferenceEntityId;
        }

        private boolean addBytesAndMaybePause(long count, IOControl ioControl) {
            circuitBreaker.addEstimateBytesAndMaybeBreak(count, inferenceEntityId);
            if (bytesInQueue.addAndGet(count) >= settings.getMaxResponseSize().getBytes()) {
                pauseProducer(ioControl);
                return true;
            }
            return false;
        }

        private void pauseProducer(IOControl ioControl) {
            synchronized (ioLock) {
                if (shutdown) {
                    // shutdownProducer() was called while we were consuming this chunk, before the IOControl
                    // was saved. Shut down now; otherwise the suspended producer would never be resumed and
                    // the leased connection would be held indefinitely.
                    doShutdown(ioControl);
                } else {
                    ioControl.suspendInput();
                    savedIoControl = ioControl;
                }
            }
        }

        private void subtractBytesAndMaybeUnpause(long count) {
            // claim from the accumulator first, and only what is actually there — a concurrent releaseTrackedBytes()
            // may already have returned these bytes to the breaker
            var before = bytesInQueue.getAndUpdate(current -> Long.max(0, current - count));
            var claimed = Long.min(count, before);
            if (claimed > 0) {
                circuitBreaker.addWithoutBreaking(-claimed);
            }
            var currentBytesInQueue = before - claimed;
            if (savedIoControl != null) {
                var maxBytes = settings.getMaxResponseSize().getBytes() * 0.5;
                if (currentBytesInQueue <= maxBytes) {
                    resumeProducer();
                }
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

        private void releaseTrackedBytes() {
            var remaining = bytesInQueue.getAndSet(0);
            if (remaining > 0) {
                circuitBreaker.addWithoutBreaking(-remaining);
            }
        }

        private void shutdownProducer() {
            synchronized (ioLock) {
                shutdown = true;
                if (savedIoControl != null) {
                    doShutdown(savedIoControl);
                    savedIoControl = null;
                }
            }
        }

        private void doShutdown(IOControl ioControl) {
            try {
                ioControl.shutdown();
            } catch (IOException e) {
                logger.warn("Failed to shut down paused Apache producer after subscription cancellation", e);
            }
        }
    }
}
