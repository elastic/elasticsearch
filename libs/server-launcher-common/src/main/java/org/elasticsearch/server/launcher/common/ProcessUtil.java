/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.launcher.common;

/**
 * Utility methods for dealing with interruptible operations in a no-interruption-policy context.
 */
public class ProcessUtil {

    private ProcessUtil() { /* no instance */ }

    public interface Interruptible<T> {
        T run() throws InterruptedException;
    }

    public interface InterruptibleVoid {
        void run() throws InterruptedException;
    }

    /**
     * Runs an interruptable method, but throws an assertion if an interrupt is received.
     *
     * This is useful for threads which expect a no interruption policy
     */
    public static <T> T nonInterruptible(Interruptible<T> interruptible) {
        try {
            return interruptible.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    public static void nonInterruptibleVoid(InterruptibleVoid interruptible) {
        nonInterruptible(() -> {
            interruptible.run();
            return null;
        });
    }
}
