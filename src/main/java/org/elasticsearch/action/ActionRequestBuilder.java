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

package org.elasticsearch.action;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.internal.InternalGenericClient;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public abstract class ActionRequestBuilder<Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder> {

    protected final Request request;

    protected final InternalGenericClient client;

    protected ActionRequestBuilder(InternalGenericClient client, Request request) {
        this.client = client;
        this.request = request;
    }

    public Request request() {
        return this.request;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setListenerThreaded(boolean listenerThreaded) {
        request.listenerThreaded(listenerThreaded);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder putHeader(String key, Object value) {
        request.putHeader(key, value);
        return (RequestBuilder) this;
    }

    public ListenableActionFuture<Response> execute() {
        PlainListenableActionFuture<Response> future = new PlainListenableActionFuture<Response>(request.listenerThreaded(), client.threadPool());
        execute(future);
        return future;
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get() throws ElasticSearchException {
        return execute().actionGet();
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get(TimeValue timeout) throws ElasticSearchException {
        return execute().actionGet(timeout);
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get(String timeout) throws ElasticSearchException {
        return execute().actionGet(timeout);
    }

    public void execute(ActionListener<Response> listener) {
        doExecute(listener);
    }

    protected abstract void doExecute(ActionListener<Response> listener);
}
