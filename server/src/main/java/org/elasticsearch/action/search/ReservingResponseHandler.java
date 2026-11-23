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
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;

import java.util.concurrent.Executor;

/**
 * Charges a response from another node from when it starts arriving until it is read, see {@link ShardResponseReservation}. A trip on
 * arrival reaches the listener as the {@link CircuitBreakingException} itself rather than wrapped in a transport exception, since it is
 * this node that has no room and not the other node that failed.
 */
public class ReservingResponseHandler<Response extends TransportResponse> extends ActionListenerResponseHandler<Response> {
    /** The response's reservation, whose {@link ShardResponseReservation#reservedTotal()} is the size it arrived with once read. */
    protected final ShardResponseReservation reservation;

    /**
     * @param label breaker label for the reservation
     */
    public ReservingResponseHandler(
        ActionListener<? super Response> listener,
        Writeable.Reader<Response> reader,
        Executor executor,
        CircuitBreaker circuitBreaker,
        String label
    ) {
        super(listener, reader, executor);
        this.reservation = new ShardResponseReservation(circuitBreaker, label, listener::onFailure);
    }

    @Override
    public Releasable reserveResponseBytes(int bytes) {
        return reservation.reserve(bytes);
    }

    @Override
    public void handleException(TransportException e) {
        if (reservation.reportTrip() == false) {
            super.handleException(e);
        }
    }
}
