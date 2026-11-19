/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.core.Releasable;

import java.util.OptionalInt;
import java.util.Random;

/**
 * {@link UUIDSource} for tests, seeded per suite and per test method by {@link TestUUIDSourceRule}.
 */
public class TestUUIDSource implements UUIDSource {

    private static volatile UUIDSource delegate = new TimeBasedUUIDSource(
        UUIDs.DEFAULT_TIMESTAMP_SUPPLIER,
        UUIDs.DEFAULT_SEQUENCE_ID_SUPPLIER,
        UUIDs.DEFAULT_MAC_ADDRESS_SUPPLIER
    );

    static Releasable withSeed(long seed) {
        final var random = new Random(seed);
        final long timestamp = random.nextLong(1L << 48);
        final byte[] macAddress = new byte[6];
        random.nextBytes(macAddress);
        final var source = new TimeBasedUUIDSource(() -> timestamp, random::nextInt, () -> macAddress);
        final var previous = delegate;
        delegate = source;
        return () -> delegate = previous;
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
