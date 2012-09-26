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

package org.elasticsearch.client.node;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.*;
import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.InternalClusterAdminClient;
import org.elasticsearch.client.support.AbstractClusterAdminClient;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

/**
 *
 */
public class NodeClusterAdminClient extends AbstractClusterAdminClient implements InternalClusterAdminClient {

    private final ThreadPool threadPool;

    private final ImmutableMap<ClusterAction, TransportAction> actions;

    @Inject
    public NodeClusterAdminClient(Settings settings, ThreadPool threadPool, Map<GenericAction, TransportAction> actions) {
        this.threadPool = threadPool;
        MapBuilder<ClusterAction, TransportAction> actionsBuilder = new MapBuilder<ClusterAction, TransportAction>();
        for (Map.Entry<GenericAction, TransportAction> entry : actions.entrySet()) {
            if (entry.getKey() instanceof ClusterAction) {
                actionsBuilder.put((ClusterAction) entry.getKey(), entry.getValue());
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
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(ClusterAction<Request, Response, RequestBuilder> action, Request request) {
        TransportAction<Request, Response> transportAction = actions.get(action);
        return transportAction.execute(request);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(ClusterAction<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        TransportAction<Request, Response> transportAction = actions.get(action);
        transportAction.execute(request, listener);
    }
}
