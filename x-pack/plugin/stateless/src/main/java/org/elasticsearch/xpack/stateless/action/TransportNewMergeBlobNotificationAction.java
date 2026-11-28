/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.unpromotable.TransportBroadcastUnpromotableAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/// Notifies all the search shards of a shard, including relocation targets, that a merge blob was uploaded, so they can prefetch it
/// before the merge is published.
public class TransportNewMergeBlobNotificationAction extends TransportBroadcastUnpromotableAction<
    NewMergeBlobNotificationRequest,
    ActionResponse.Empty> {

    public static final String NAME = "internal:admin/stateless/search/new/merge_blob";
    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(NAME);

    @Inject
    public TransportNewMergeBlobNotificationAction(
        ClusterService clusterService,
        TransportService transportService,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters
    ) {
        super(
            NAME,
            clusterService,
            transportService,
            shardStateAction,
            actionFilters,
            NewMergeBlobNotificationRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void unpromotableShardOperation(
        Task task,
        NewMergeBlobNotificationRequest request,
        ActionListener<ActionResponse.Empty> listener
    ) {
        listener.onResponse(ActionResponse.Empty.INSTANCE);
    }

    @Override
    protected ActionResponse.Empty combineUnpromotableShardResponses(List<ActionResponse.Empty> responses) {
        return ActionResponse.Empty.INSTANCE;
    }

    @Override
    protected ActionResponse.Empty readResponse(StreamInput in) {
        return ActionResponse.Empty.INSTANCE;
    }

    @Override
    protected ActionResponse.Empty emptyResponse() {
        return ActionResponse.Empty.INSTANCE;
    }
}
