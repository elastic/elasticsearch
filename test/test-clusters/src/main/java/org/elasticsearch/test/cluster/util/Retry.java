/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.util;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

public final class Retry {
    private Retry() {}

    public static void retryUntilTrue(Duration timeout, Duration delay, BooleanSupplier predicate) throws TimeoutException,
        ExecutionException {

        long delayMs = delay.toMillis();
        AtomicReference<Exception> exception = new AtomicReference<>();
        Thread t = new Thread(() -> {
            for (;;) {
                try {
                    boolean complete = predicate.getAsBoolean();
                    if (complete) {
                        return;
                    }
                } catch (Exception e) {
                    exception.set(e);
                    return;
                }

                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "Retry");
        t.start();

        try {
            t.join(timeout.toMillis());
            if (t.isAlive()) {
                // it didn't complete in time
                throw new TimeoutException();
            }
            if (exception.get() != null) {
                throw new ExecutionException(exception.get());
            }
        } catch (InterruptedException e) {
            // this thread was interrupted while waiting for t to stop
            throw new TimeoutException();
        } finally {
            // stop the thread if it's still running
            t.interrupt();
        }
    }
}
