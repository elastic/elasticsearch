/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction.Request;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction.Response;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TransportGetCheckpointNodeAction extends HandledTransportAction<Request, Response> {

    private final IndicesService indicesService;

    @Inject
    public TransportGetCheckpointNodeAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final IndicesService indicesService
    ) {
        super(GetCheckpointNodeAction.NAME, transportService, actionFilters, Request::new);
        this.indicesService = indicesService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        getGlobalCheckpoints(indicesService, request.getShards(), listener);
    }

    protected static void getGlobalCheckpoints(IndicesService indicesService, Set<ShardId> shards, ActionListener<Response> listener) {
        Map<String, long[]> checkpointsByIndexOfThisNode = new HashMap<>();
        for (ShardId shardId : shards) {
            final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            final IndexShard indexShard = indexService.getShard(shardId.id());

            checkpointsByIndexOfThisNode.computeIfAbsent(shardId.getIndexName(), k -> {
                long[] seqNumbers = new long[indexService.getIndexSettings().getNumberOfShards()];
                Arrays.fill(seqNumbers, SequenceNumbers.UNASSIGNED_SEQ_NO);
                return seqNumbers;
            });
            checkpointsByIndexOfThisNode.get(shardId.getIndexName())[shardId.getId()] = indexShard.seqNoStats().getGlobalCheckpoint();
        }
        listener.onResponse(new Response(checkpointsByIndexOfThisNode));
    }
}
