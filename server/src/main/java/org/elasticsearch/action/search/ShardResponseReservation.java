/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.transport.TransportResponseHandler;

import java.util.function.Consumer;

/**
 * Charges a shard response to the coordinating node's request breaker from when it starts arriving until it is read, see
 * {@link TransportResponseHandler#reserveResponseBytes}. A trip means this node has no room for the response, not that the shard
 * failed, so it is reported to the search, which fails as a whole instead of retrying the shard on another copy. A remote cluster's
 * response in a CCS search with minimized round trips is charged the same way.
 * <p>
 * Holds the state of one response, so each request needs its own.
 */
public final class ShardResponseReservation {

    /** Label for a fetch response, which carries hits. */
    public static final String FETCH_BREAKER_LABEL = ChildMemoryCircuitBreaker.CATEGORY_FETCH + "[shard_response]";

    /** Label for the responses of the other phases. */
    public static final String BREAKER_LABEL = "<shard_response>";

    private final CircuitBreaker circuitBreaker;
    private final String label;
    private final Consumer<Exception> onTrip;
    private volatile CircuitBreakingException trip;

    /**
     * @param onTrip fails the search, called on the response handler's executor
     */
    public ShardResponseReservation(CircuitBreaker circuitBreaker, String label, Consumer<Exception> onTrip) {
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
        return Releasables.assertOnce(() -> circuitBreaker.addWithoutBreaking(-bytes, label));
    }

    /**
     * Reports a trip in {@link #reserve} to the search. To be called first thing in
     * {@link TransportResponseHandler#handleException}, which gets the trip wrapped in a {@code RemoteTransportException}.
     *
     * @return whether the response was rejected here, in which case the caller must not count it as a shard failure
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
