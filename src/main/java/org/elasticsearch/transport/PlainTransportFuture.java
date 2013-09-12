/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchInterruptedException;
import org.elasticsearch.ElasticSearchTimeoutException;
import org.elasticsearch.common.util.concurrent.BaseFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class PlainTransportFuture<V extends TransportResponse> extends BaseFuture<V> implements TransportFuture<V>, TransportResponseHandler<V> {

    private final TransportResponseHandler<V> handler;

    public PlainTransportFuture(TransportResponseHandler<V> handler) {
        this.handler = handler;
    }

    @Override
    public V txGet() throws ElasticSearchException {
        try {
            return get();
        } catch (InterruptedException e) {
            throw new ElasticSearchInterruptedException(e.getMessage());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ElasticSearchException) {
                throw (ElasticSearchException) e.getCause();
            } else {
                throw new TransportException("Failed execution", e);
            }
        }
    }

    @Override
    public V txGet(long timeout, TimeUnit unit) throws ElasticSearchException {
        try {
            return get(timeout, unit);
        } catch (TimeoutException e) {
            throw new ElasticSearchTimeoutException(e.getMessage());
        } catch (InterruptedException e) {
            throw new ElasticSearchInterruptedException(e.getMessage());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ElasticSearchException) {
                throw (ElasticSearchException) e.getCause();
            } else {
                throw new TransportException("Failed execution", e);
            }
        }
    }

    @Override
    public V newInstance() {
        return handler.newInstance();
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
        } catch (Throwable t) {
            handleException(new ResponseHandlerFailureTransportException(t));
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
