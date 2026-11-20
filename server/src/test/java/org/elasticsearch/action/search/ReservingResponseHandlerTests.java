/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class ReservingResponseHandlerTests extends ESTestCase {

    /** The handler charges a response's bytes from when they arrive until the transport releases them. */
    public void testReservesResponseBytes() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofKb(1));
        var handler = handler(ActionListener.noop(), breaker);
        Releasable reservation = handler.reserveResponseBytes(100);
        assertThat(breaker.getUsed(), equalTo(100L));
        reservation.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * A trip reserving a response reaches the listener as the trip itself, which marks it as this node's failure rather than the other
     * node's. Other transport failures pass through unchanged.
     */
    public void testReportsItsOwnTrip() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(10));
        AtomicReference<Exception> tripFailure = new AtomicReference<>();
        var trippingHandler = handler(ActionListener.wrap(r -> fail("unexpected response"), tripFailure::set), breaker);
        CircuitBreakingException trip = expectThrows(CircuitBreakingException.class, () -> trippingHandler.reserveResponseBytes(100));
        trippingHandler.handleException(new RemoteTransportException("Failed to reserve memory", trip));
        assertSame(trip, tripFailure.get());
        assertThat(breaker.getUsed(), equalTo(0L));

        AtomicReference<Exception> otherFailure = new AtomicReference<>();
        var handler = handler(ActionListener.wrap(r -> fail("unexpected response"), otherFailure::set), breaker);
        TransportException failure = new RemoteTransportException("remote failed", new ElasticsearchException("boom"));
        handler.handleException(failure);
        assertSame(failure, otherFailure.get());
    }

    private static ReservingResponseHandler<SearchResponse> handler(ActionListener<SearchResponse> listener, CircuitBreaker breaker) {
        return new ReservingResponseHandler<>(
            listener,
            SearchResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            breaker,
            ShardResponseReservation.BREAKER_LABEL
        );
    }
}
