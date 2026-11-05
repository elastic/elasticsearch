/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportResponseHandler;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ShardResponseReservationTests extends ESTestCase {

    /** Limit of the breaker that a response trips in the tests of the routes that reserve for it. */
    static final ByteSizeValue TRIPPING_LIMIT = ByteSizeValue.ofBytes(100);

    private static final int OVER_LIMIT_BYTES = 1000;

    /**
     * Does what the transport does when a piece of the response trips a breaker of {@link #TRIPPING_LIMIT}. The reservation records the
     * trip, and the handler reports it and fails the listener.
     *
     * @return the trip
     */
    static CircuitBreakingException tripWhileArriving(ShardResponseReservation reservation, ActionListener<?> listener) {
        CircuitBreakingException trip = expectThrows(CircuitBreakingException.class, () -> reservation.reserve(OVER_LIMIT_BYTES));
        assertTrue(reservation.reportTrip());
        listener.onFailure(new RemoteTransportException("rejected", trip));
        return trip;
    }

    /**
     * Same as {@link #tripWhileArriving(ShardResponseReservation, ActionListener)}, through the handler the response is for.
     */
    static CircuitBreakingException tripWhileArriving(TransportResponseHandler<?> handler) {
        CircuitBreakingException trip = expectThrows(CircuitBreakingException.class, () -> handler.reserveResponseBytes(OVER_LIMIT_BYTES));
        handler.handleException(new RemoteTransportException("rejected", trip));
        return trip;
    }

    public void testReservesEachPieceUntilReleased() {
        CircuitBreaker breaker = newLimitedBreaker(TRIPPING_LIMIT);
        List<Exception> trips = new ArrayList<>();
        ShardResponseReservation reservation = new ShardResponseReservation(breaker, ShardResponseReservation.BREAKER_LABEL, trips::add);

        Releasable first = reservation.reserve(40);
        Releasable second = reservation.reserve(60);
        assertThat(breaker.getUsed(), equalTo(100L));
        Releasables.close(first, second);
        assertThat(breaker.getUsed(), equalTo(0L));

        assertFalse(reservation.reportTrip());
        assertThat(trips, empty());
    }

    public void testReportsTripToTheSearch() {
        CircuitBreaker breaker = newLimitedBreaker(TRIPPING_LIMIT);
        List<Exception> trips = new ArrayList<>();
        ShardResponseReservation reservation = new ShardResponseReservation(breaker, ShardResponseReservation.BREAKER_LABEL, trips::add);

        Releasable first = reservation.reserve(60);
        CircuitBreakingException trip = expectThrows(CircuitBreakingException.class, () -> reservation.reserve(60));
        // What arrived before the trip stays reserved until the transport drops the response
        assertThat(breaker.getUsed(), equalTo(60L));
        first.close();
        assertThat(breaker.getUsed(), equalTo(0L));

        // Reported only once the handler gets the failure, after the transport is done with the response
        assertThat(trips, empty());
        assertTrue(reservation.reportTrip());
        assertThat(trips, contains(sameInstance(trip)));
    }
}
