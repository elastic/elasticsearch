/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.core.CheckedRunnable;

import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Placeholder for a seeded {@link UUIDSource} in tests. Currently delegates to the time-based generators and proves that
 * {@link UUIDs} resolves a source supplied by the test framework.
 */
public class TestUUIDSource implements UUIDSource {

    private static final UUIDSource DEFAULT = new TimeBasedUUIDSource(
        UUIDs.DEFAULT_TIMESTAMP_SUPPLIER,
        UUIDs.DEFAULT_SEQUENCE_ID_SUPPLIER,
        UUIDs.DEFAULT_MAC_ADDRESS_SUPPLIER
    );

    private static final AtomicReference<UUIDSource> delegate = new AtomicReference<>(DEFAULT);

    /**
     * Routes {@link UUIDs#base64UUID()} and {@link UUIDs#base64TimeBasedKOrderedUUIDWithHash} across the JVM, including
     * internal cluster nodes, to {@code source} while {@code body} runs, then restores the time-based generators. Allows a
     * single active scope per JVM and throws {@link AssertionError} on a second one, nested or concurrent.
     */
    public static <E extends Exception> void withUUIDSource(UUIDSource source, CheckedRunnable<E> body) throws E {
        if (delegate.compareAndSet(DEFAULT, source) == false) {
            throw new AssertionError("another withUUIDSource scope is active with [" + delegate.get() + "]");
        }
        try {
            body.run();
        } finally {
            delegate.set(DEFAULT);
        }
    }

    @Override
    public String base64UUID() {
        return delegate.get().base64UUID();
    }

    @Override
    public String base64TimeBasedKOrderedUUIDWithHash(OptionalInt hash) {
        return delegate.get().base64TimeBasedKOrderedUUIDWithHash(hash);
    }
}
