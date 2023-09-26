/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;

public interface TransportResponseHandler<T extends TransportResponse> extends Writeable.Reader<T> {

    /**
     * @return the executor to use to deserialize the response and notify the listener. You must only use
     * {@link EsExecutors#DIRECT_EXECUTOR_SERVICE} (or equivalently {@link TransportResponseHandler#TRANSPORT_WORKER}) for very
     * performance-critical actions, and even then only if the deserialization and handling work is very cheap, because this executor will
     * perform all the work for responses from remote nodes on the receiving transport worker itself.
     */
    Executor executor(ThreadPool threadPool);

    void handleResponse(T response);

    void handleException(TransportException exp);

    /**
     * Implementation of {@link TransportResponseHandler} that handles the empty response {@link TransportResponse.Empty}.
     */
    abstract class Empty implements TransportResponseHandler<TransportResponse.Empty> {
        @Override
        public final TransportResponse.Empty read(StreamInput in) {
            return TransportResponse.Empty.INSTANCE;
        }

        @Override
        public final void handleResponse(TransportResponse.Empty ignored) {
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
            public Executor executor(ThreadPool threadPool) {
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
