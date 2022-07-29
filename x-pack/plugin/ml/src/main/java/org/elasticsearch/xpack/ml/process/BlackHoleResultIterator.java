/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.process;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A custom iterator that blocks even when there are no results as
 * a {@link java.util.concurrent.LinkedBlockingDeque} iterator and stream aren't.
 * @param <T> the result type
 */
public class BlackHoleResultIterator<T> implements Iterator<T> {

    private final BlockingQueue<T> results;
    private final Supplier<Boolean> isRunning;
    private volatile T latestResult;

    public BlackHoleResultIterator(BlockingQueue<T> results, Supplier<Boolean> isRunning) {
        this.results = results;
        this.isRunning = isRunning;
    }

    @Override
    public boolean hasNext() {
        try {
            while (isRunning.get()) {
                latestResult = results.poll(100, TimeUnit.MILLISECONDS);
                if (latestResult != null) {
                    return true;
                }
            }
            latestResult = results.poll();
            return latestResult != null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public T next() {
        return latestResult;
    }
}
