/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.Executor;

public interface TransportResponseHandler<T extends TransportResponse> extends Writeable.Reader<T> {

    /**
     * @return the executor to use to deserialize the response and notify the listener. You must only use
     * {@link EsExecutors#DIRECT_EXECUTOR_SERVICE} (or equivalently {@link TransportResponseHandler#TRANSPORT_WORKER}) for very
     * performance-critical actions, and even then only if the deserialization and handling work is very cheap, because this executor will
     * perform all the work for responses from remote nodes on the receiving transport worker itself.
     */
    Executor executor();

    void handleResponse(T response);

    void handleException(TransportException exp);

    /**
     * Reserves memory for the next {@code bytes} of a response to this handler. Called on the transport worker for each piece of the
     * response as it arrives, whatever {@link #executor()} is, and not for an error response. The reservations are closed when the response
     * is dropped or, if it is read, before {@link #handleResponse}, so the handler can then charge what it keeps. They may be closed on a
     * different thread from the one that reserved them.
     *
     * @return the reservation, or {@link #NO_RESERVATION} to reserve nothing for the rest of the response
     * @throws org.elasticsearch.common.breaker.CircuitBreakingException to reject the response: the rest is dropped unread, and
     *         {@link #handleException} gets the exception on {@link #executor()}, wrapped in a {@link RemoteTransportException} like a trip
     *         on the remote node. A handler that needs to tell them apart records its own.
     */
    default Releasable reserveResponseBytes(int bytes) {
        return NO_RESERVATION;
    }

    /**
     * Returned by {@link #reserveResponseBytes} to reserve nothing.
     */
    Releasable NO_RESERVATION = () -> {};

    /**
     * Implementation of {@link TransportResponseHandler} that handles the empty response {@link ActionResponse.Empty}.
     */
    abstract class Empty implements TransportResponseHandler<ActionResponse.Empty> {
        @Override
        public final ActionResponse.Empty read(StreamInput in) {
            return ActionResponse.Empty.INSTANCE;
        }

        @Override
        public final void handleResponse(ActionResponse.Empty ignored) {
            handleResponse();
        }

        public abstract void handleResponse();
    }

    static Empty empty(Executor executor, ActionListener<Void> listener) {
        return new Empty() {
            @Override
            public void handleResponse() {
                listener.onResponse(null);
            }

            @Override
            public Executor executor() {
                return executor;
            }

            @Override
            public void handleException(TransportException exp) {
                listener.onFailure(exp);
            }

            @Override
            public String toString() {
                return listener.toString();
            }
        };
    }

    /**
     * Alias for {@link EsExecutors#DIRECT_EXECUTOR_SERVICE} which emphasises that non-forking transport response handlers do their work on
     * the transport worker (unless the request is being sent to the local node, but that's not the common case). You must only use this
     * executor for handling responses to very performance-critical actions, and even then only if the deserialization and handling work is
     * very cheap, because this executor will perform all the work for responses from remote nodes on the receiving transport worker itself.
     */
    Executor TRANSPORT_WORKER = EsExecutors.DIRECT_EXECUTOR_SERVICE;
}
