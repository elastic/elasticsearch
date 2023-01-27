/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportUnpromotableShardRefreshAction extends HandledTransportAction<UnpromotableShardRefreshRequest, ActionResponse.Empty> {
    public static final String NAME = RefreshAction.NAME + "[u]";
    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(NAME, in -> ActionResponse.Empty.INSTANCE);
    private static final Logger logger = LogManager.getLogger(TransportUnpromotableShardRefreshAction.class);
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndicesService indicesService;
    private final Client client;

    @Inject
    public TransportUnpromotableShardRefreshAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        Client client
    ) {
        super(NAME, transportService, actionFilters, UnpromotableShardRefreshRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, UnpromotableShardRefreshRequest request, ActionListener<ActionResponse.Empty> listener) {
        try {
            assert request.getSegmentGeneration() != Engine.RefreshResult.UNKNOWN_GENERATION;
            IndexShard shard = indicesService.indexServiceSafe(request.getShardId().getIndex()).getShard(request.getShardId().id());
            shard.waitForSegmentGeneration(request.getSegmentGeneration(), listener.map(l -> ActionResponse.Empty.INSTANCE));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
