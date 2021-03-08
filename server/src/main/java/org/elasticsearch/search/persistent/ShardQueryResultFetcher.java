/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.search.persistent.GetShardResultAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.function.Function;

public class ShardQueryResultFetcher {
    private final TransportService transportService;
    private final Function<String, Transport.Connection> connectionProvider;

    public ShardQueryResultFetcher(TransportService transportService,
                                   Function<String, Transport.Connection> connectionProvider) {
        this.transportService = transportService;
        this.connectionProvider = connectionProvider;
    }

    public ShardSearchResult getSearchShardResultBlocking(String id, String nodeId, Task task, TimeValue timeout) {
        PlainActionFuture<ShardSearchResult> future = PlainActionFuture.newFuture();
        getSearchShardResultAsync(id, nodeId, task, future);
        return future.actionGet(timeout);
    }

    public void getSearchShardResultAsync(String id, String nodeId, Task task, ActionListener<ShardSearchResult> listener) {
        final Transport.Connection connection = connectionProvider.apply(nodeId);

        if (connection == null) {
            throw new IllegalArgumentException("Unable to find a node with id [" + nodeId + "]");
        }

        final ActionListener<GetShardResultAction.Response> mappedListener =
            listener.map(GetShardResultAction.Response::getShardSearchResult);

        transportService.sendChildRequest(connection, GetShardResultAction.NAME, new GetShardResultAction.Request(id),
            task, new ActionListenerResponseHandler<>(mappedListener, GetShardResultAction.Response::new));
    }
}
