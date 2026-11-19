/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Charges what the objects of an inbound transport request keep beyond its serialized message to the request breaker as they are read,
 * see {@link StreamInput#chargeDecodedBytes}, and releases it with the message's in-flight charge once the response is sent, so a request
 * is charged at its decoded size while it waits to run, not only while it runs.
 *
 * <p>The breaker is checked each time {@link #CHUNK_BYTES} are charged and once the request has been read, not on every charge, so a
 * request read into many small pieces does not check the parent breaker's real memory for each.
 */
final class DecodedRequestBytes implements Releasable {

    static final long CHUNK_BYTES = ByteSizeUnit.MB.toBytes(1);

    private final Supplier<CircuitBreaker> breakerSupplier;
    private final boolean canTrip;
    private final Releasable inFlightRelease;
    // Set when the response is sent, on any thread. Volatile because the supplier the stream returns reads it without the lock.
    private volatile boolean released;

    // The fields below are guarded by this. They are set on the first charge, on the thread reading the request, and stay unset for a
    // request that is never charged.
    @Nullable
    private CircuitBreaker breaker;
    @Nullable
    private Map<String, Long> chargedByLabel;
    private long uncheckedBytes;
    @Nullable
    private String uncheckedLabel;

    /**
     * @param breakerSupplier the request breaker, looked up on the first charge
     * @param canTrip whether a charge may trip the breaker, false for handlers that must not reject a request for the memory it uses
     * @param inFlightRelease releases the in-flight charge of the message, when this is released
     */
    DecodedRequestBytes(Supplier<CircuitBreaker> breakerSupplier, boolean canTrip, Releasable inFlightRelease) {
        this.breakerSupplier = breakerSupplier;
        this.canTrip = canTrip;
        this.inFlightRelease = inFlightRelease;
    }

    /**
     * A stream to read the request from, which charges what is read from it to this.
     */
    StreamInput streamInput(StreamInput delegate, NamedWriteableRegistry namedWriteableRegistry) {
        return new NamedWriteableAwareStreamInput(delegate, namedWriteableRegistry) {
            @Override
            public BooleanSupplier chargeDecodedBytes(long bytes, String label) {
                charge(bytes, label);
                return () -> released == false;
            }
        };
    }

    private synchronized void charge(long bytes, String label) {
        assert bytes >= 0 : bytes;
        assert released == false : "charged after the response was sent";
        if (breaker == null) {
            breaker = breakerSupplier.get();
            chargedByLabel = new HashMap<>(1);
        }
        if (canTrip && uncheckedBytes + bytes >= CHUNK_BYTES) {
            // Also checks what was charged without a check before, since the breaker checks its whole use
            breaker.addEstimateBytesAndMaybeBreak(bytes, label);
            uncheckedBytes = 0;
        } else {
            breaker.addWithoutBreaking(bytes, label);
            uncheckedBytes += bytes;
            uncheckedLabel = label;
        }
        chargedByLabel.merge(label, bytes, Long::sum);
    }

    /**
     * Checks the breaker once the request has been read if charges are still unchecked, so a request under a chunk is checked before it
     * waits to run. Adds no bytes, and a trip leaves what was charged held until this is released.
     */
    synchronized void checkRead() {
        if (canTrip && uncheckedBytes > 0) {
            uncheckedBytes = 0;
            breaker.addEstimateBytesAndMaybeBreak(0L, uncheckedLabel);
        }
    }

    @Override
    public void close() {
        try {
            synchronized (this) {
                released = true;
                if (chargedByLabel != null) {
                    chargedByLabel.forEach((label, bytes) -> breaker.addWithoutBreaking(-bytes, label));
                    chargedByLabel.clear();
                }
            }
        } finally {
            inFlightRelease.close();
        }
    }
}
