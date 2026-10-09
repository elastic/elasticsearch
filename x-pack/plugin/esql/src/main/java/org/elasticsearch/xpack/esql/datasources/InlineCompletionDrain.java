/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.ArrayDeque;
import java.util.List;

/**
 * Runs grant completions on the calling thread. A completion that {@code release()}s and
 * grants the next waiter enqueues that delivery on this thread's drain instead of
 * recursing, so a chain of cancelled heads cannot blow the stack.
 */
final class InlineCompletionDrain {

    private static final ThreadLocal<ArrayDeque<Runnable>> QUEUE = ThreadLocal.withInitial(ArrayDeque::new);
    private static final ThreadLocal<Boolean> RUNNING = ThreadLocal.withInitial(() -> Boolean.FALSE);

    private InlineCompletionDrain() {}

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
        RuntimeException firstException = null;
        Error firstError = null;
        try {
            Runnable next;
            while ((next = queue.pollFirst()) != null) {
                try {
                    next.run();
                } catch (RuntimeException e) {
                    if (firstException == null) {
                        firstException = e;
                    } else {
                        firstException.addSuppressed(e);
                    }
                } catch (Error e) {
                    if (firstError == null) {
                        firstError = e;
                    } else {
                        firstError.addSuppressed(e);
                    }
                }
            }
        } finally {
            RUNNING.set(Boolean.FALSE);
            QUEUE.remove();
            RUNNING.remove();
        }
        if (firstError != null) {
            throw firstError;
        }
        if (firstException != null) {
            throw firstException;
        }
    }
}
