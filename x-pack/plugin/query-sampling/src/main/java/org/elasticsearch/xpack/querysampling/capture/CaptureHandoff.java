/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.capture;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Hands captures to an executor; if its bounded queue is full the capture is dropped, never blocking the search. A capture whose
 * processing fails is counted and logged, at most once a minute, and the thread of the executor goes on with the next one.
 */
public final class CaptureHandoff implements Consumer<CapturedSearch> {

    private static final Logger logger = LogManager.getLogger(CaptureHandoff.class);
    private static final long NEVER = Long.MIN_VALUE;
    private static final long WARN_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);

    private final Executor executor;
    private final Consumer<CapturedSearch> downstream;
    private final LongSupplier millis;
    private final LongAdder dropped = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final AtomicLong lastWarning = new AtomicLong(NEVER);

    public CaptureHandoff(Executor executor, Consumer<CapturedSearch> downstream, LongSupplier millis) {
        this.executor = executor;
        this.downstream = downstream;
        this.millis = millis;
    }

    @Override
    public void accept(CapturedSearch captured) {
        try {
            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    downstream.accept(captured);
                }

                @Override
                public void onFailure(Exception e) {
                    failed.increment();
                    warn(e);
                }

                @Override
                public void onRejection(Exception e) {
                    dropped.increment();
                }
            });
        } catch (RejectedExecutionException e) {
            dropped.increment(); // an executor that rejects by throwing, rather than through onRejection
        }
    }

    private void warn(Exception e) {
        long now = millis.getAsLong();
        long last = lastWarning.get();
        if ((last == NEVER || now - last >= WARN_INTERVAL_MILLIS) && lastWarning.compareAndSet(last, now)) {
            logger.warn("query sampling failed to process a captured search (logged at most once a minute)", e);
        }
    }

    public long dropped() {
        return dropped.sum();
    }

    public long failed() {
        return failed.sum();
    }
}
