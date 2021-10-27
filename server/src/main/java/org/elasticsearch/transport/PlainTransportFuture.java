/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.BaseFuture;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PlainTransportFuture<V extends TransportResponse> extends BaseFuture<V>
    implements
        TransportFuture<V>,
        TransportResponseHandler<V> {

    private final TransportResponseHandler<V> handler;

    public PlainTransportFuture(TransportResponseHandler<V> handler) {
        this.handler = handler;
    }

    @Override
    public V txGet() {
        try {
            return get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Future got interrupted", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ElasticsearchException) {
                throw (ElasticsearchException) e.getCause();
            } else {
                throw new TransportException("Failed execution", e);
            }
        }
    }

    @Override
    public V txGet(long timeout, TimeUnit unit) {
        try {
            return get(timeout, unit);
        } catch (TimeoutException e) {
            throw new ElasticsearchTimeoutException(e);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Future got interrupted", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ElasticsearchException) {
                throw (ElasticsearchException) e.getCause();
            } else {
                throw new TransportException("Failed execution", e);
            }
        }
    }

    @Override
    public V read(StreamInput in) throws IOException {
        return handler.read(in);
    }

    @Override
    public String executor() {
        return handler.executor();
    }

    @Override
    public void handleResponse(V response) {
        try {
            handler.handleResponse(response);
            set(response);
        } catch (Exception e) {
            handleException(new ResponseHandlerFailureTransportException(e));
        }
    }

    @Override
    public void handleException(TransportException exp) {
        try {
            handler.handleException(exp);
        } finally {
            setException(exp);
        }
    }

    @Override
    public String toString() {
        return "future(" + handler.toString() + ")";
    }
}
