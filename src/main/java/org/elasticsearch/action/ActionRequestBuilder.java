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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

/**
 *
 */
public abstract class ActionRequestBuilder<Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder, Client extends ElasticsearchClient> {

    protected final Request request;
    private final ThreadPool threadPool;
    protected final Client client;

    protected ActionRequestBuilder(Client client, Request request) {
        this.request = request;
        this.client = client;
        threadPool = client.threadPool();
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
        PlainListenableActionFuture<Response> future = new PlainListenableActionFuture<>(request.listenerThreaded(), threadPool);
        execute(future);
        return future;
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get() throws ElasticsearchException {
        return execute().actionGet();
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get(TimeValue timeout) throws ElasticsearchException {
        return execute().actionGet(timeout);
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get(String timeout) throws ElasticsearchException {
        return execute().actionGet(timeout);
    }

    public void execute(ActionListener<Response> listener) {
        doExecute(listener);
    }

    protected abstract void doExecute(ActionListener<Response> listener);
}
