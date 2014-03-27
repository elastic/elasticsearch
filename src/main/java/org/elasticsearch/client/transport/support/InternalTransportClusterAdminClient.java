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

package org.elasticsearch.client.transport.support;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.*;
import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.internal.InternalClusterAdminClient;
import org.elasticsearch.client.support.AbstractClusterAdminClient;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

/**
 *
 */
@SuppressWarnings("unchecked")
public class InternalTransportClusterAdminClient extends AbstractClusterAdminClient implements InternalClusterAdminClient {

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
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(final ClusterAction<Request, Response, RequestBuilder> action, final Request request) {
        final TransportActionNodeProxy<Request, Response> proxy = actions.get(action);
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<Response>>() {
            @Override
            public ActionFuture<Response> doWithNode(DiscoveryNode node) throws ElasticsearchException {
                return proxy.execute(node, request);
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(final ClusterAction<Request, Response, RequestBuilder> action, final Request request, final ActionListener<Response> listener) {
        final TransportActionNodeProxy<Request, Response> proxy = actions.get(action);
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<Response>() {
            @Override
            public void doWithNode(DiscoveryNode node, ActionListener<Response> listener) throws ElasticsearchException {
                proxy.execute(node, request, listener);
            }
        }, listener);
    }

}
