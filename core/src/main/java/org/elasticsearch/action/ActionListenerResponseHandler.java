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

package org.elasticsearch.action;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * A simple base class for action response listeners, defaulting to using the SAME executor (as its
 * very common on response handlers).
 */
public class ActionListenerResponseHandler<Response extends TransportResponse> extends BaseTransportResponseHandler<Response> {

    private final ActionListener<Response> listener;
    private final Supplier<Response> responseSupplier;

    public ActionListenerResponseHandler(ActionListener<Response> listener, Supplier<Response> responseSupplier) {
        this.listener = Objects.requireNonNull(listener);
        this.responseSupplier = Objects.requireNonNull(responseSupplier);
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
    public Response newInstance() {
        return responseSupplier.get();
    }

    @Override
    public String executor() {
        return ThreadPool.Names.SAME;
    }
}
