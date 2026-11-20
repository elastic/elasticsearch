/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.staging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A few attempts at a repository call that fails for a moment: a throttled request, a reset connection, a credential that is being
 * renewed. The staged upload of a bulk is on the path that acknowledges it, so one such failure would fail a client request that the
 * next try would have served.
 *
 * <p>The number of attempts is small and the waits are short, because a write thread of the shard blocks while this waits.
 */
public final class Retries {

    /** One attempt. {@code attempt} counts from 0. */
    @FunctionalInterface
    public interface Attempt<T> {
        T run(int attempt) throws IOException;
    }

    /** The wait between two attempts, which the tests replace so that they do not sleep. */
    @FunctionalInterface
    public interface Pause {
        void sleep(long millis) throws InterruptedException;
    }

    private Retries() {}

    /**
     * Runs {@code attempt} until it returns, at most {@code attempts} times. After a failure it waits {@code firstDelayMillis}, and four
     * times as long after each next one. Only an {@link IOException} or a {@link RuntimeException} is tried again. An {@link Error}
     * is thrown at once, as is an interruption (which ends the waiting and keeps the flag of the thread).
     *
     * @throws IOException the exception of the last attempt, with the exceptions of the earlier attempts added as suppressed ones, as an
     *                     {@link IOException} or a {@link RuntimeException} the way the attempt threw it
     */
    public static <T> T run(int attempts, long firstDelayMillis, Attempt<T> attempt, Pause pause) throws IOException {
        if (attempts < 1) {
            throw new IllegalArgumentException("attempts must be at least 1 but was [" + attempts + "]");
        }
        final List<Exception> earlier = new ArrayList<>();
        Exception last = null;
        long delay = firstDelayMillis;
        for (int number = 0; number < attempts; number++) {
            try {
                return attempt.run(number);
            } catch (IOException | RuntimeException e) {
                if (last != null && last != e) {
                    earlier.add(last);
                }
                last = e;
            }
            if (number < attempts - 1) {
                try {
                    pause.sleep(delay);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    last.addSuppressed(interrupted);
                    break;
                }
                delay *= 4;
            }
        }
        for (Exception failure : earlier) {
            last.addSuppressed(failure); // flat, in the order of the attempts, and not a chain of exceptions that each hold the one before
        }
        if (last instanceof IOException io) {
            throw io;
        }
        throw (RuntimeException) last;
    }
}
