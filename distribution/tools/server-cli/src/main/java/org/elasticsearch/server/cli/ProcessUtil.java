/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

class ProcessUtil {

    private ProcessUtil() { /* no instance*/ }

    interface Interruptible<T> {
        T run() throws InterruptedException;
    }

    interface InterruptibleVoid {
        void run() throws InterruptedException;
    }

    /**
     * Runs an interruptable method, but throws an assertion if an interrupt is received.
     *
     * This is useful for threads which expect a no interruption policy
     */
    static <T> T nonInterruptible(Interruptible<T> interruptible) {
        try {
            return interruptible.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    static void nonInterruptibleVoid(InterruptibleVoid interruptible) {
        nonInterruptible(() -> {
            interruptible.run();
            return null;
        });
    }
}
