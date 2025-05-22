/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Processor that delegates the {@link java.util.concurrent.Flow.Subscription} to the upstream {@link java.util.concurrent.Flow.Publisher}
 * and delegates event transmission to the downstream {@link java.util.concurrent.Flow.Subscriber}.
 */
public abstract class DelegatingProcessor<T, R> implements Flow.Processor<T, R> {
    private static final Logger log = LogManager.getLogger(DelegatingProcessor.class);
    private final AtomicLong pendingRequests = new AtomicLong();
    protected final AtomicBoolean isClosed = new AtomicBoolean(false);
    private Flow.Subscriber<? super R> downstream;
    private Flow.Subscription upstream;

    public static <ParsedChunk> Deque<ParsedChunk> parseEvent(
        Deque<ServerSentEvent> item,
        ParseChunkFunction<ParsedChunk> parseFunction,
        XContentParserConfiguration parserConfig,
        Logger logger
    ) throws Exception {
        var results = new ArrayDeque<ParsedChunk>(item.size());
        for (ServerSentEvent event : item) {
            if (event.hasData()) {
                try {
                    var delta = parseFunction.apply(parserConfig, event);
                    delta.forEachRemaining(results::offer);
                } catch (Exception e) {
                    logger.warn("Failed to parse event from inference provider: {}", event);
                    throw e;
                }
            }
        }

        return results;
    }

    @FunctionalInterface
    public interface ParseChunkFunction<ParsedChunk> {
        Iterator<ParsedChunk> apply(XContentParserConfiguration parserConfig, ServerSentEvent event) throws IOException;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super R> subscriber) {
        if (downstream != null) {
            subscriber.onError(new IllegalStateException("Another subscriber is already subscribed."));
            return;
        }

        var subscription = forwardingSubscription();
        try {
            downstream = subscriber;
            downstream.onSubscribe(subscription);
        } catch (Exception e) {
            log.atDebug().withThrowable(e).log("Another publisher is already publishing to subscriber, canceling.");
            subscription.cancel();
            downstream = null;
            throw e;
        }
    }

    private Flow.Subscription forwardingSubscription() {
        return new Flow.Subscription() {
            @Override
            public void request(long n) {
                if (isClosed.get()) {
                    downstream.onComplete();
                } else if (upstream != null) {
                    upstreamRequest(n);
                } else {
                    pendingRequests.accumulateAndGet(n, Long::sum);
                }
            }

            @Override
            public void cancel() {
                if (isClosed.compareAndSet(false, true) && upstream != null) {
                    upstream.cancel();
                    onCancel();
                }
            }
        };
    }

    /**
     * Guaranteed to be called when the upstream is set and this processor had not been closed.
     */
    protected void upstreamRequest(long n) {
        upstream.request(n);
    }

    protected void onCancel() {}

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (upstream != null) {
            throw new IllegalStateException("Another upstream already exists. This subscriber can only subscribe to one publisher.");
        }

        if (isClosed.get()) {
            subscription.cancel();
            return;
        }

        upstream = subscription;
        var currentRequestCount = pendingRequests.getAndSet(0);
        if (currentRequestCount != 0) {
            upstream.request(currentRequestCount);
        }
    }

    @Override
    public void onNext(T item) {
        if (isClosed.get()) {
            upstream.cancel();
        } else {
            try {
                next(item);
            } catch (Exception e) {
                upstream().cancel();
                onError(e);
            }
        }
    }

    /**
     * An {@link #onNext(Object)} that is only called when the stream is still open.
     * Implementations can pass the resulting R object to the downstream subscriber via {@link #downstream()}, or the upstream can be
     * accessed via {@link #upstream()}.
     * Any Exceptions thrown by this method will cancel the upstream and be sent to the downstream {@link #onError(Throwable)}.
     */
    protected abstract void next(T item) throws Exception;

    @Override
    public void onError(Throwable throwable) {
        if (isClosed.compareAndSet(false, true)) {
            if (downstream != null) {
                downstream.onError(throwable);
            } else {
                log.atDebug()
                    .withThrowable(throwable)
                    .log("onError was called before the downstream subscription, rethrowing to close listener.");
                throw new IllegalStateException("onError was called before the downstream subscription", throwable);
            }
        }
    }

    @Override
    public void onComplete() {
        if (isClosed.compareAndSet(false, true)) {
            if (downstream != null) {
                downstream.onComplete();
            }
        }
    }

    protected Flow.Subscriber<? super R> downstream() {
        return downstream;
    }

    protected Flow.Subscription upstream() {
        return upstream;
    }
}
