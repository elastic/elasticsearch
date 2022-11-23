/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.util.Objects;

/**
 * A simple base class for action response listeners, defaulting to using the SAME executor (as its
 * very common on response handlers).
 */
public class ActionListenerResponseHandler<Response extends TransportResponse> implements TransportResponseHandler<Response> {

    protected final ActionListener<? super Response> listener;
    private final Writeable.Reader<Response> reader;
    private final String executor;

    public ActionListenerResponseHandler(ActionListener<? super Response> listener, Writeable.Reader<Response> reader, String executor) {
        this.listener = Objects.requireNonNull(listener);
        this.reader = Objects.requireNonNull(reader);
        this.executor = Objects.requireNonNull(executor);
    }

    public ActionListenerResponseHandler(ActionListener<? super Response> listener, Writeable.Reader<Response> reader) {
        this(listener, reader, ThreadPool.Names.SAME);
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
    public String executor() {
        return executor;
    }

    @Override
    public Response read(StreamInput in) throws IOException {
        return reader.read(in);
    }

    @Override
    public String toString() {
        return super.toString() + "/" + listener;
    }
}
