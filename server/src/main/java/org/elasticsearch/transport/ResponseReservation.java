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
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Charges a response from another node to the breaker that the caller passes in, from when it starts arriving until it is read, see
 * {@link TransportResponseHandler#reserveResponseBytes}. A trip means this node has no room for the response, not that the other node
 * failed, and it is reported through the {@code onTrip} callback, which decides what fails: a search fails as a whole instead of retrying
 * the shard on another copy, while a task listing fails only the node whose reply did not fit.
 * <p>
 * Holds the state of one response, so each request needs its own.
 */
public final class ResponseReservation {

    private final CircuitBreaker circuitBreaker;
    private final String label;
    private final Consumer<Exception> onTrip;
    private final AtomicLong reservedTotal = new AtomicLong();
    private volatile CircuitBreakingException trip;

    /**
     * @param onTrip fails the request, called on the response handler's executor
     */
    public ResponseReservation(CircuitBreaker circuitBreaker, String label, Consumer<Exception> onTrip) {
        this.circuitBreaker = circuitBreaker;
        this.label = label;
        this.onTrip = onTrip;
    }

    /**
     * Implements {@link TransportResponseHandler#reserveResponseBytes}.
     */
    public Releasable reserve(int bytes) {
        try {
            circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, label);
        } catch (CircuitBreakingException e) {
            trip = e;
            throw e;
        }
        reservedTotal.addAndGet(bytes);
        return Releasables.assertOnce(() -> circuitBreaker.addWithoutBreaking(-bytes, label));
    }

    /**
     * The bytes reserved so far, released or not. Once the response is read, this is the size it arrived with.
     */
    public long reservedTotal() {
        return reservedTotal.get();
    }

    /**
     * Reports a trip in {@link #reserve} to the request. To be called first thing in
     * {@link TransportResponseHandler#handleException}, which gets the trip wrapped in a {@code RemoteTransportException}.
     *
     * @return whether the response was rejected here, in which case the caller must not count it as a failure of the other node
     */
    public boolean reportTrip() {
        CircuitBreakingException e = trip;
        if (e == null) {
            return false;
        }
        onTrip.accept(e);
        return true;
    }
}
