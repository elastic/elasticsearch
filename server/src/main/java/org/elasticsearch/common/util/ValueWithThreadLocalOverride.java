/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import java.util.function.Supplier;

/**
 * Stores value and provides optional functionality to set up a per-thread override value.
 * Intended usage is a singleton value that is commonly accessed from multiple places.
 * Having it as singleton allows to not pass instance down to every consumer.
 * Thread-local override allows to use different value in tests even though it is a singleton.
 * Inspired by <a href="https://docs.rs/tracing/latest/tracing/dispatcher/index.html">tracing</a>.
 */
public class ValueWithThreadLocalOverride<T> {
    // Intentionally not static - different values should allow different overrides.
    private final ThreadLocal<T> threadLocal = ThreadLocal.withInitial(() -> null);
    private Supplier<T> supplier;

    public ValueWithThreadLocalOverride(T value) {
        this.supplier = () -> value;
    }

    /**
     * returns stored value or an override if set
     * @return T
     */
    public T get() {
        return supplier.get();
    }

    /**
     * Installs a thread-local override value.
     * @param value
     * @return an {@link AutoCloseable} that removes the override.
     */
    public AutoCloseable withOverride(T value) {
        threadLocal.set(value);
        // This is a small optimization to eliminate thread local lookup
        // if override was never set, which is most of the time.
        T original = supplier.get();
        this.supplier = () -> getWithOverride(original);

        return new Reset(threadLocal);
    }

    private T getWithOverride(T original) {
        var local = threadLocal.get();
        if (local != null) {
            return local;
        }

        return original;
    }

    private record Reset(ThreadLocal<?> threadLocal) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            threadLocal.remove();
        }
    }
}
