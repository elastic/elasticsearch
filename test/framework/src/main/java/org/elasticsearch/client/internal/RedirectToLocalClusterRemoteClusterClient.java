/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportResponse;

/**
 * A fake {@link RemoteClusterClient} which just runs actions on the local cluster, like a {@link NodeClient}, for use in tests.
 */
public class RedirectToLocalClusterRemoteClusterClient implements RemoteClusterClient {

    private final ElasticsearchClient localNodeClient;

    public RedirectToLocalClusterRemoteClusterClient(ElasticsearchClient localNodeClient) {
        this.localNodeClient = localNodeClient;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends TransportResponse> void execute(
        RemoteClusterActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        localNodeClient.execute(new ActionType<>(action.name()), request, listener.map(r -> (Response) r));
    }

    @Override
    public <Request extends ActionRequest, Response extends TransportResponse> void execute(
        Transport.Connection connection,
        RemoteClusterActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        throw new AssertionError("not implemented on RedirectToLocalClusterRemoteClusterClient");
    }

    @Override
    public <Request extends ActionRequest> void getConnection(Request request, ActionListener<Transport.Connection> listener) {
        throw new AssertionError("not implemented on RedirectToLocalClusterRemoteClusterClient");
    }
}
