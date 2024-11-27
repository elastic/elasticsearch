/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;

class StreamingResponseHandler implements Flow.Processor<HttpResult, HttpResult> {
    private static final Logger log = LogManager.getLogger(StreamingResponseHandler.class);
    private final ThrottlerManager throttlerManager;
    private final Logger throttlerLogger;
    private final Request request;
    private final ResponseHandler responseHandler;

    private final AtomicBoolean upstreamIsClosed = new AtomicBoolean(false);
    private final AtomicBoolean processedFirstItem = new AtomicBoolean(false);

    private volatile Flow.Subscription upstream;
    private volatile Flow.Subscriber<? super HttpResult> downstream;

    StreamingResponseHandler(ThrottlerManager throttlerManager, Logger throttlerLogger, Request request, ResponseHandler responseHandler) {
        this.throttlerManager = throttlerManager;
        this.throttlerLogger = throttlerLogger;
        this.request = request;
        this.responseHandler = responseHandler;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super HttpResult> subscriber) {
        if (downstream != null) {
            subscriber.onError(
                new IllegalStateException("Failed to initialize streaming response. Another subscriber is already subscribed.")
            );
            return;
        }

        downstream = subscriber;
        subscriber.onSubscribe(forwardingSubscription());
    }

    private Flow.Subscription forwardingSubscription() {
        return new Flow.Subscription() {
            @Override
            public void request(long n) {
                if (upstreamIsClosed.get()) {
                    downstream.onComplete(); // shouldn't happen, but reinforce that we're no longer listening
                } else if (upstream != null) {
                    upstream.request(n);
                } else {
                    // this shouldn't happen, the expected call pattern is onNext -> subscribe after the listener is invoked
                    var errorMessage = "Failed to initialize streaming response. onSubscribe must be called first to set the upstream";
                    assert false : errorMessage;
                    downstream.onError(new IllegalStateException(errorMessage));
                }
            }

            @Override
            public void cancel() {
                if (upstreamIsClosed.compareAndSet(false, true) && upstream != null) {
                    upstream.cancel();
                }
            }
        };
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        upstream = subscription;
    }

    @Override
    public void onNext(HttpResult item) {
        if (processedFirstItem.compareAndSet(false, true)) {
            try {
                responseHandler.validateResponse(throttlerManager, throttlerLogger, request, item);
            } catch (Exception e) {
                logException(throttlerLogger, request, item, responseHandler.getRequestType(), e);
                upstream.cancel();
                onError(e);
                return;
            }
        }
        downstream.onNext(item);
    }

    @Override
    public void onError(Throwable throwable) {
        if (upstreamIsClosed.compareAndSet(false, true)) {
            if (downstream != null) {
                downstream.onError(throwable);
            } else {
                log.warn(
                    "Flow failed before the InferenceServiceResults were generated.  The error should go to the listener directly.",
                    throwable
                );
            }
        }
    }

    @Override
    public void onComplete() {
        if (upstreamIsClosed.compareAndSet(false, true)) {
            if (downstream != null) {
                downstream.onComplete();
            } else {
                log.debug("Flow completed before the InferenceServiceResults were generated.  Shutting down this Processor.");
            }
        }
    }

    private void logException(Logger logger, Request request, HttpResult result, String requestType, Exception exception) {
        var causeException = ExceptionsHelper.unwrapCause(exception);

        throttlerManager.warn(
            logger,
            format(
                "Failed to process the stream connection for request from inference entity id [%s] of type [%s] with status [%s] [%s]",
                request.getInferenceEntityId(),
                requestType,
                result.response().getStatusLine().getStatusCode(),
                result.response().getStatusLine().getReasonPhrase()
            ),
            causeException
        );
    }
}
