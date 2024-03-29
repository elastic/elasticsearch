/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.metrics;

import java.util.Objects;

/**
 * Provides easy access to metrics (or anything really)
 * without the need to pass metrics object explicitly.
 * Intended to use as a singleton in consumers but still allows injecting test values
 * by first checking thread local value before falling back to set value.
 */
public class MetricAccessor<T> {
    // Intentionally not static - we expect users to only create instance
    // per each "global" resource.
    private ThreadLocal<T> threadLocal = ThreadLocal.withInitial(() -> null);
    private T value;

    public MetricAccessor(T value) {
        this.value = Objects.requireNonNull(value);
    }

    public AutoCloseable initForTest(T value) {
        threadLocal.set(value);

        return new Reset(threadLocal);
    }

    /**
     * returns stored value or thread local value if set
     * @return T
     */
    public T get() {
        var local = threadLocal.get();
        if (local != null) {
            return local;
        }

        return value;
    }

    private record Reset(ThreadLocal<?> threadLocal) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            threadLocal.remove();
        }
    }
}
