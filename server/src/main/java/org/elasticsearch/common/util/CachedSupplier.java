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
 * A {@link Supplier} that caches its return value. This may be useful to make
 * a {@link Supplier} idempotent or for performance reasons if always returning
 * the same instance is acceptable.
 */
public final class CachedSupplier<T> implements Supplier<T> {

    private final Supplier<T> supplier;
    private volatile T result;
    private volatile boolean resultSet;

    public CachedSupplier(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    @Override
    public T get() {
        if (resultSet == false) {
            synchronized (this) {
                if (resultSet == false) {
                    result = supplier.get();
                    resultSet = true;
                }
            }
        }
        return result;
    }

}
