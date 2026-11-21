/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.capture;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/** Hands captures to an executor; if its bounded queue is full the capture is dropped, never blocking the search. */
public final class CaptureHandoff implements Consumer<CapturedSearch> {

    private final Executor executor;
    private final Consumer<CapturedSearch> downstream;
    private final LongAdder dropped = new LongAdder();

    public CaptureHandoff(Executor executor, Consumer<CapturedSearch> downstream) {
        this.executor = executor;
        this.downstream = downstream;
    }

    @Override
    public void accept(CapturedSearch captured) {
        try {
            executor.execute(() -> downstream.accept(captured));
        } catch (RejectedExecutionException e) {
            dropped.increment();
        }
    }

    public long dropped() {
        return dropped.sum();
    }
}
