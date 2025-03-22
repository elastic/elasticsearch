/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.IndexShard;

/**
 * This class is used to assert that a thread does not access the current engine reference using the method
 * {@link IndexShard#getEngineOrNull()} when it is executing some protected code blocks. A protected code block is a
 * block of code which starts when {@link #accessStart()} is called and ends when {@link #accessEnd()} is called.
 */
public final class SafeEngineAccessThreadLocal {

    @Nullable // if assertions are disabled
    private static final ThreadLocal<Accessor> threadLocalAccessor;
    static {
        threadLocalAccessor = Assertions.ENABLED ? new ThreadLocal<>() : null;
    }

    private static class Accessor {

        private final Thread thread;
        private final SetOnce<AssertionError> failure;

        private Accessor(Thread thread) {
            this.thread = thread;
            this.failure = new SetOnce<>();
        }

        private void setFailure(AssertionError error) {
            failure.set(error);
        }

        private boolean isFailed() {
            return failure.get() != null;
        }

        private AssertionError getFailure() {
            return failure.get();
        }

        @Override
        public String toString() {
            return thread.toString();
        }
    }

    private SafeEngineAccessThreadLocal() {}

    private static Accessor getAccessorSafe() {
        final var accessor = threadLocalAccessor.get();
        if (accessor != null && accessor.isFailed()) {
            throw new AssertionError("Current thread has made an unsafe access to the engine", accessor.getFailure());
        }
        return accessor;
    }

    public static void accessStart() {
        ensureAssertionsEnabled();
        final var accessor = getAccessorSafe();
        assert accessor == null : "current accessor already set";
        threadLocalAccessor.set(new Accessor(Thread.currentThread()));
    }

    public static void accessEnd() {
        ensureAssertionsEnabled();
        final var accessor = getAccessorSafe();
        assert accessor != null : "current accessor not set";
        var thread = Thread.currentThread();
        assert accessor.thread == thread : "current accessor [" + accessor + "] was set by a different thread [" + thread + ']';
        threadLocalAccessor.remove();
    }

    /**
     * Use this method to assert that the current thread has not entered a protected execution code block.
     */
    public static boolean assertSafeAccess() {
        ensureAssertionsEnabled();
        final var accessor = getAccessorSafe();
        if (accessor != null) {
            var message = "thread [" + accessor + "] should not access the engine using the getEngineOrNull() method";
            accessor.setFailure(new AssertionError(message)); // to be thrown later
            assert false : message;
            return false;
        }
        return true;
    }

    private static void ensureAssertionsEnabled() {
        if (Assertions.ENABLED == false) {
            throw new AssertionError("Only use this method when assertions are enabled");
        }
        assert threadLocalAccessor != null;
    }
}
