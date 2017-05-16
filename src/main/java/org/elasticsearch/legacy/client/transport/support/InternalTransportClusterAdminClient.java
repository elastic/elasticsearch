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

package org.elasticsearch.legacy.client.transport.support;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.legacy.action.*;
import org.elasticsearch.legacy.action.admin.cluster.ClusterAction;
import org.elasticsearch.legacy.action.support.PlainActionFuture;
import org.elasticsearch.legacy.client.ClusterAdminClient;
import org.elasticsearch.legacy.client.support.AbstractClusterAdminClient;
import org.elasticsearch.legacy.client.transport.TransportClientNodesService;
import org.elasticsearch.legacy.cluster.node.DiscoveryNode;
import org.elasticsearch.legacy.common.collect.MapBuilder;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

import java.util.Map;

/**
 *
 */
@SuppressWarnings("unchecked")
public class InternalTransportClusterAdminClient extends AbstractClusterAdminClient implements ClusterAdminClient {

    private final TransportClientNodesService nodesService;

    private final ThreadPool threadPool;

    private final ImmutableMap<ClusterAction, TransportActionNodeProxy> actions;

    @Inject
    public InternalTransportClusterAdminClient(Settings settings, TransportClientNodesService nodesService, ThreadPool threadPool, TransportService transportService,
                                               Map<String, GenericAction> actions) {
        this.nodesService = nodesService;
        this.threadPool = threadPool;
        MapBuilder<ClusterAction, TransportActionNodeProxy> actionsBuilder = new MapBuilder<>();
        for (GenericAction action : actions.values()) {
            if (action instanceof ClusterAction) {
                actionsBuilder.put((ClusterAction) action, new TransportActionNodeProxy(settings, action, transportService));
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
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ClusterAdminClient>> ActionFuture<Response> execute(final Action<Request, Response, RequestBuilder, ClusterAdminClient> action, final Request request) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
        execute(action, request, actionFuture);
        return actionFuture;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ClusterAdminClient>> void execute(final Action<Request, Response, RequestBuilder, ClusterAdminClient> action, final Request request, final ActionListener<Response> listener) {
        final TransportActionNodeProxy<Request, Response> proxy = actions.get(action);
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<Response>() {
            @Override
            public void doWithNode(DiscoveryNode node, ActionListener<Response> listener) {
                proxy.execute(node, request, listener);
            }
        }, listener);
    }

}
