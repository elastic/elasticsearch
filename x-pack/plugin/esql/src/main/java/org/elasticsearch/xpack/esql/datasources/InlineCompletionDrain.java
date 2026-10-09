/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayDeque;
import java.util.List;

/**
 * Runs grant completions on the calling thread. A completion that {@code release()}s and
 * grants the next waiter enqueues that delivery on this thread's drain instead of
 * recursing, so a chain of cancelled heads cannot blow the stack.
 * <p>
 * Each completion is isolated: a throw does not skip later grants and does not propagate
 * out of {@link #run}, so a GET {@code onResponse} that {@code release()}s still delivers
 * the buffer. Fatal {@link Error}s are rethrown on another thread.
 */
public final class InlineCompletionDrain {

    private static final Logger logger = LogManager.getLogger(InlineCompletionDrain.class);

    private static final ThreadLocal<ArrayDeque<Runnable>> QUEUE = ThreadLocal.withInitial(ArrayDeque::new);
    private static final ThreadLocal<Boolean> RUNNING = ThreadLocal.withInitial(() -> Boolean.FALSE);

    private InlineCompletionDrain() {}

    /** {@code true} while this thread is draining grant completions. */
    public static boolean draining() {
        return Boolean.TRUE.equals(RUNNING.get());
    }

    static void run(List<Runnable> completions) {
        if (completions.isEmpty()) {
            return;
        }
        ArrayDeque<Runnable> queue = QUEUE.get();
        queue.addAll(completions);
        if (RUNNING.get()) {
            return;
        }
        RUNNING.set(Boolean.TRUE);
        try {
            Runnable next;
            while ((next = queue.pollFirst()) != null) {
                try {
                    next.run();
                } catch (Exception e) {
                    logger.warn("grant completion failed", e);
                } catch (Error e) {
                    logger.error("grant completion error", e);
                    ExceptionsHelper.maybeDieOnAnotherThread(e);
                }
            }
        } finally {
            RUNNING.set(Boolean.FALSE);
            QUEUE.remove();
            RUNNING.remove();
        }
    }
}
