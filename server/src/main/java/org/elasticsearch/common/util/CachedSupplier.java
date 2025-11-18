/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import java.util.function.Supplier;

/**
 * A {@link Supplier} that caches its return value. This may be useful to make
 * a {@link Supplier} idempotent or for performance reasons if always returning
 * the same instance is acceptable.
 */
public final class CachedSupplier<T> implements Supplier<T> {

    private volatile Supplier<T> supplier;
    // result does not need to be volatile as we only read it after reading that the supplier got nulled out. Since we null out the
    // supplier after setting the result, total store order from an observed volatile write is sufficient to make a plain read safe.
    private T result;

    public static <R> CachedSupplier<R> wrap(Supplier<R> supplier) {
        if (supplier instanceof CachedSupplier<R> c) {
            // no need to wrap a cached supplier again
            return c;
        }
        return new CachedSupplier<>(supplier);
    }

    private CachedSupplier(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    @Override
    public T get() {
        if (supplier == null) {
            return result;
        }
        return initResult();
    }

    private synchronized T initResult() {
        var s = supplier;
        if (s != null) {
            T res = s.get();
            result = res;
            supplier = null;
            return res;
        } else {
            return result;
        }
    }

}
