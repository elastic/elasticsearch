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

package org.elasticsearch.client.node;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.*;
import org.elasticsearch.action.admin.indices.IndicesAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.support.AbstractIndicesAdminClient;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

/**
 *
 */
public class NodeIndicesAdminClient extends AbstractIndicesAdminClient implements IndicesAdminClient {

    private final ThreadPool threadPool;

    private final ImmutableMap<IndicesAction, TransportAction> actions;

    private final Headers headers;

    @Inject
    public NodeIndicesAdminClient(ThreadPool threadPool, Map<GenericAction, TransportAction> actions, Headers headers) {
        this.threadPool = threadPool;
        this.headers = headers;
        MapBuilder<IndicesAction, TransportAction> actionsBuilder = new MapBuilder<>();
        for (Map.Entry<GenericAction, TransportAction> entry : actions.entrySet()) {
            if (entry.getKey() instanceof IndicesAction) {
                actionsBuilder.put((IndicesAction) entry.getKey(), entry.getValue());
            }
        }
        this.actions = actionsBuilder.immutableMap();
    }

    @Override
    public ThreadPool threadPool() {
        return this.threadPool;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, IndicesAdminClient>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, IndicesAdminClient> action, Request request) {
        headers.applyTo(request);
        TransportAction<Request, Response> transportAction = actions.get((IndicesAction)action);
        return transportAction.execute(request);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, IndicesAdminClient>> void execute(Action<Request, Response, RequestBuilder, IndicesAdminClient> action, Request request, ActionListener<Response> listener) {
        headers.applyTo(request);
        TransportAction<Request, Response> transportAction = actions.get((IndicesAction)action);
        transportAction.execute(request, listener);
    }
}
