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

/**
 * Moves captured searches off the search thread. The queue in front of the executor is bounded and a
 * search never waits for room in it: when the queue is full the capture is dropped and counted.
 * Losing a capture only costs a little statistical precision, slowing down a search would cost much more.
 */
public final class CaptureHandoff implements Consumer<CapturedSearch> {

    private final Executor executor;
    private final Consumer<CapturedSearch> downstream;
    private final LongAdder handedOff = new LongAdder();
    private final LongAdder dropped = new LongAdder();

    public CaptureHandoff(Executor executor, Consumer<CapturedSearch> downstream) {
        this.executor = executor;
        this.downstream = downstream;
    }

    @Override
    public void accept(CapturedSearch captured) {
        try {
            executor.execute(() -> downstream.accept(captured));
            handedOff.increment();
        } catch (RejectedExecutionException e) {
            dropped.increment();
        }
    }

    public long handedOff() {
        return handedOff.sum();
    }

    public long dropped() {
        return dropped.sum();
    }
}
