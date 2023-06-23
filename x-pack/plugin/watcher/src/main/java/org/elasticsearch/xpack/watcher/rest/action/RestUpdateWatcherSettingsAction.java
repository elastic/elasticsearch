/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.UpdateWatcherSettingsAction;

import java.io.IOException;
import java.util.List;

/**
 * Allows setting a subset of index settings for the .watches index.
 * See {@link RestGetWatcherSettingsAction} for the retrieval counterpart.
 */
public class RestUpdateWatcherSettingsAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "watcher_update_settings";
    }

    @Override
    public List<Route> routes() {
        return List.of(Route.builder(RestRequest.Method.PUT, "/_watcher/settings").build());
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        UpdateWatcherSettingsAction.Request req = new UpdateWatcherSettingsAction.Request(request.contentParser().map());
        return channel -> client.execute(UpdateWatcherSettingsAction.INSTANCE, req, new RestToXContentListener<>(channel));
    }
}
