/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * An adapter for handling transport responses using an {@link ActionListener}.
 */
public class ActionListenerResponseHandler<Response extends TransportResponse> implements TransportResponseHandler<Response> {

    protected final ActionListener<? super Response> listener;
    private final Writeable.Reader<Response> reader;
    private final Executor executor;

    /**
     * @param listener The listener to notify with the transport response received.
     * @param reader   Used to deserialize the response.
     * @param executor The executor to use to deserialize the response and notify the listener. You must only use
     *                 {@link EsExecutors#DIRECT_EXECUTOR_SERVICE} (or equivalently {@link TransportResponseHandler#TRANSPORT_WORKER})
     *                 for very performance-critical actions, and even then only if the deserialization and handling work is very cheap,
     *                 because this executor will perform all the work for responses from remote nodes on the receiving transport worker
     *                 itself.
     */
    public ActionListenerResponseHandler(ActionListener<? super Response> listener, Writeable.Reader<Response> reader, Executor executor) {
        this.listener = Objects.requireNonNull(listener);
        this.reader = Objects.requireNonNull(reader);
        this.executor = Objects.requireNonNull(executor);
    }

    @Override
    public void handleResponse(Response response) {
        listener.onResponse(response);
    }

    @Override
    public void handleException(TransportException e) {
        listener.onFailure(e);
    }

    @Override
    public Executor executor() {
        return executor;
    }

    @Override
    public Response read(StreamInput in) throws IOException {
        return reader.read(in);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '[' + listener + ']';
    }
}
