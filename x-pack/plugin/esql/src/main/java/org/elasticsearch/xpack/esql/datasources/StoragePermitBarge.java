/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.io.IOException;

/**
 * Marks a {@link org.elasticsearch.xpack.esql.datasources.spi.StorageObject#newStream} as a text
 * resume re-open so {@link ConcurrencyLimitedStorageObject} barges ({@link ConcurrencyLimiter#tryAcquire()})
 * instead of parking on {@link ConcurrencyLimiter#acquireChecked()}. The first GET of a stream still
 * uses the blocking acquire; only {@link RetryableStorageObject}'s mid-stream resume hops here.
 * Misses poll on the reader (barge+poll), they do not {@code acquireAsync().join()}.
 */
final class StoragePermitBarge {

    private static final ThreadLocal<Boolean> ACTIVE = new ThreadLocal<>();

    private StoragePermitBarge() {}

    static boolean active() {
        return Boolean.TRUE.equals(ACTIVE.get());
    }

    static <T> T call(IOCallable<T> action) throws IOException {
        Boolean previous = ACTIVE.get();
        ACTIVE.set(Boolean.TRUE);
        try {
            return action.call();
        } finally {
            if (previous == null) {
                ACTIVE.remove();
            } else {
                ACTIVE.set(previous);
            }
        }
    }

    @FunctionalInterface
    interface IOCallable<T> {
        T call() throws IOException;
    }
}
