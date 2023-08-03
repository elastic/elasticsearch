/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;

public class EmptyTransportResponseHandler extends TransportResponseHandler.Empty {

    private final ActionListener<Void> listener;

    public EmptyTransportResponseHandler(ActionListener<Void> listener) {
        this.listener = listener;
    }

    @Override
    public Executor executor(ThreadPool threadPool) {
        return TransportResponseHandler.TRANSPORT_WORKER;
    }

    @Override
    public final void handleResponse(TransportResponse.Empty response) {
        listener.onResponse(null);
    }

    @Override
    public final void handleException(TransportException exp) {
        listener.onFailure(exp);
    }
}
