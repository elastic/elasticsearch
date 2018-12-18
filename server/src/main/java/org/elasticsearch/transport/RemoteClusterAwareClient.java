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

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

final class RemoteClusterAwareClient extends AbstractClient implements RemoteClient {

    private final TransportService service;
    private final String clusterAlias;
    private final RemoteClusterService remoteClusterService;
    private final DiscoveryNode discoveryNode;

    RemoteClusterAwareClient(Settings settings, ThreadPool threadPool, TransportService service, String clusterAlias) {
        this(settings, threadPool, service, clusterAlias, null);
    }

    private RemoteClusterAwareClient(Settings settings, ThreadPool threadPool, TransportService service, String clusterAlias,
                                     DiscoveryNode discoveryNode) {
        super(settings, threadPool);
        this.service = service;
        this.clusterAlias = clusterAlias;
        this.remoteClusterService = service.getRemoteClusterService();
        this.discoveryNode = discoveryNode;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse>
    void doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
        remoteClusterService.ensureConnected(clusterAlias, ActionListener.wrap(res -> {
            Transport.Connection connection;
            if (discoveryNode == null) {
                connection = remoteClusterService.getConnection(clusterAlias);
            } else {
                connection = remoteClusterService.getConnection(discoveryNode, clusterAlias);
            }
            service.sendRequest(connection, action.name(), request, TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, action.getResponseReader()));
        },
        listener::onFailure));
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public RemoteClient getRemoteClusterClient(String clusterAlias) {
        return remoteClusterService.getRemoteClusterClient(threadPool(), clusterAlias);
    }

    @Override
    public Client preferredNode(DiscoveryNode node) {
        return new RemoteClusterAwareClient(settings, threadPool(), service, clusterAlias, node);
    }
}
