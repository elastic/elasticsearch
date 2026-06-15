/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

class RequestTask implements RejectableTask {

    private final RequestManager requestCreator;
    private final InferenceInputs inferenceInputs;
    private final TimedListener<InferenceServiceResults> timedListener;
    private final CircuitBreaker circuitBreaker;

    RequestTask(
        RequestManager requestCreator,
        InferenceInputs inferenceInputs,
        @Nullable TimeValue timeout,
        ThreadPool threadPool,
        ActionListener<InferenceServiceResults> listener,
        CircuitBreaker circuitBreaker,
        long estimatedRamBytesUsed
    ) {
        this.requestCreator = Objects.requireNonNull(requestCreator);
        this.timedListener = new TimedListener<>(
            timeout,
            releaseInflightBytesOnceListener(listener, circuitBreaker, estimatedRamBytesUsed),
            threadPool,
            requestCreator.inferenceEntityId()
        );
        this.inferenceInputs = Objects.requireNonNull(inferenceInputs);
        this.circuitBreaker = Objects.requireNonNull(circuitBreaker);
    }

    @Override
    public boolean hasCompleted() {
        return timedListener.hasCompleted();
    }

    @Override
    public Supplier<Boolean> getRequestCompletedFunction() {
        return this::hasCompleted;
    }

    @Override
    public InferenceInputs getInferenceInputs() {
        return inferenceInputs;
    }

    @Override
    public ActionListener<InferenceServiceResults> getListener() {
        return timedListener.getListener();
    }

    @Override
    public void onRejection(Exception e) {
        timedListener.getListener().onFailure(e);
    }

    @Override
    public RequestManager getRequestManager() {
        return requestCreator;
    }

    // visible for testing
    CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    private static ActionListener<InferenceServiceResults> releaseInflightBytesOnceListener(
        ActionListener<InferenceServiceResults> listener,
        CircuitBreaker circuitBreaker,
        long estimatedRamBytesUsed
    ) {
        // This makes sure that the estimated RAM byte usage is only "released" once
        var ramBytesReleased = new AtomicBoolean();
        return ActionListener.runAfter(listener, () -> {
            if (ramBytesReleased.compareAndSet(false, true)) {
                circuitBreaker.addWithoutBreaking(estimatedRamBytesUsed);
            }
        });
    }
}
