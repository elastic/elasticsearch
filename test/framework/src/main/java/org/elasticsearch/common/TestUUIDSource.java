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

/**
 * Placeholder for a seeded {@link UUIDSource} in tests. Currently delegates to the time-based generators and proves that
 * {@link UUIDs} resolves a source supplied by the test framework.
 */
public class TestUUIDSource implements UUIDSource {

    private static volatile UUIDSource delegate = new TimeBasedUUIDSource(
        UUIDs.DEFAULT_TIMESTAMP_SUPPLIER,
        UUIDs.DEFAULT_SEQUENCE_ID_SUPPLIER,
        UUIDs.DEFAULT_MAC_ADDRESS_SUPPLIER
    );

    /**
     * For self-testing only. Thread-unsafe, trappy, static override.
     * It exists to test SPI class loading.
     */
    static <E extends Exception> void withUUIDSource(UUIDSource source, CheckedRunnable<E> body) throws E {
        final var previous = delegate;
        delegate = source;
        try {
            body.run();
        } finally {
            delegate = previous;
        }
    }

    @Override
    public String base64UUID() {
        return delegate.base64UUID();
    }

    @Override
    public String base64TimeBasedKOrderedUUIDWithHash(OptionalInt hash) {
        return delegate.base64TimeBasedKOrderedUUIDWithHash(hash);
    }
}
