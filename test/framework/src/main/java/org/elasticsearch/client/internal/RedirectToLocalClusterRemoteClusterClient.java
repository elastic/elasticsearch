/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;

/**
 * A fake {@link RemoteClusterClient} which just runs actions on the local cluster, like a {@link NodeClient}, for use in tests.
 */
public class RedirectToLocalClusterRemoteClusterClient implements RemoteClusterClient {

    private final ElasticsearchClient delegate;

    public RedirectToLocalClusterRemoteClusterClient(ElasticsearchClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void execute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        delegate.execute(action, request, listener);
    }
}
