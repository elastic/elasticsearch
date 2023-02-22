/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.util;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class Retry {
    private static final Executor EXECUTOR = new Executor() {
        @Override
        public void execute(Runnable command) {
            new Thread(command).start();
        }
    };

    private Retry() {}

    public static void retryUntilTrue(Duration timeout, Duration delay, Callable<Boolean> predicate) throws TimeoutException,
        ExecutionException {
        getValueWithTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS, () -> {
            while (true) {
                Boolean call = predicate.call();
                if (call) {
                    return true;
                }

                Thread.sleep(delay.toMillis());
            }
        });
    }

    private static <T> T getValueWithTimeout(long timeout, TimeUnit timeUnit, Callable<T> predicate) throws TimeoutException,
        ExecutionException {
        CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
            try {
                return predicate.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, EXECUTOR);

        try {
            return future.get(timeout, timeUnit);
        } catch (InterruptedException e) {
            throw new TimeoutException();
        }
    }
}
