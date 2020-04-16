/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.BaseFuture;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TransportFuture<V extends TransportResponse> extends BaseFuture<V> implements Future<V>, TransportResponseHandler<V> {

    private final TransportResponseHandler<V> handler;

    public TransportFuture(TransportResponseHandler<V> handler) {
        this.handler = handler;
    }

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
