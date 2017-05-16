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
import org.elasticsearch.legacy.action.support.PlainActionFuture;
import org.elasticsearch.legacy.client.AdminClient;
import org.elasticsearch.legacy.client.Client;
import org.elasticsearch.legacy.client.support.AbstractClient;
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
public class InternalTransportClient extends AbstractClient {

    private final Settings settings;
    private final ThreadPool threadPool;

    private final TransportClientNodesService nodesService;

    private final InternalTransportAdminClient adminClient;

    private final ImmutableMap<Action, TransportActionNodeProxy> actions;

    @Inject
    public InternalTransportClient(Settings settings, ThreadPool threadPool, TransportService transportService,
                                   TransportClientNodesService nodesService, InternalTransportAdminClient adminClient,
                                   Map<String, GenericAction> actions) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.nodesService = nodesService;
        this.adminClient = adminClient;
        MapBuilder<Action, TransportActionNodeProxy> actionsBuilder = new MapBuilder<>();
        for (GenericAction action : actions.values()) {
            if (action instanceof Action) {
                actionsBuilder.put((Action) action, new TransportActionNodeProxy(settings, action, transportService));
            }
        }
        this.actions = actionsBuilder.immutableMap();
    }

    @Override
    public void close() {
        // nothing to do here
    }

    @Override
    public Settings settings() {
        return this.settings;
    }

    @Override
    public ThreadPool threadPool() {
        return this.threadPool;
    }

    @Override
    public AdminClient admin() {
        return adminClient;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> ActionFuture<Response> execute(final Action<Request, Response, RequestBuilder, Client> action, final Request request) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
        execute(action, request, actionFuture);
        return actionFuture;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> void execute(final Action<Request, Response, RequestBuilder, Client> action, final Request request, ActionListener<Response> listener) {
        final TransportActionNodeProxy<Request, Response> proxy = actions.get(action);
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<Response>() {
            @Override
            public void doWithNode(DiscoveryNode node, ActionListener<Response> listener) {
                proxy.execute(node, request, listener);
            }
        }, listener);
    }
}
