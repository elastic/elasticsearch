/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction.Request;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction.Response;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TransportGetCheckpointNodeAction extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetCheckpointNodeAction.class);
    private final IndicesService indicesService;

    @Inject
    public TransportGetCheckpointNodeAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final IndicesService indicesService
    ) {
        super(GetCheckpointNodeAction.NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.indicesService = indicesService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        getGlobalCheckpoints(indicesService, task, request.getShards(), request.getTimeout(), Clock.systemUTC(), listener);
    }

    protected static void getGlobalCheckpoints(
        IndicesService indicesService,
        Task task,
        Set<ShardId> shards,
        TimeValue timeout,
        Clock clock,
        ActionListener<Response> listener
    ) {
        Map<String, long[]> checkpointsByIndexOfThisNode = new HashMap<>();
        int numProcessedShards = 0;
        for (ShardId shardId : shards) {
            if (task instanceof CancellableTask) {
                // There is no point continuing this work if the task has been cancelled.
                if (((CancellableTask) task).notifyIfCancelled(listener)) {
                    return;
                }
            }
            if (timeout != null) {
                Instant now = clock.instant();
                if (task.getStartTime() + timeout.millis() < now.toEpochMilli()) {
                    listener.onFailure(
                        new ElasticsearchTimeoutException(
                            "Transform checkpointing timed out on node [{}] after [{}] having processed [{}] of [{}] shards",
                            indicesService.clusterService().getNodeName(),
                            timeout.getStringRep(),
                            numProcessedShards,
                            shards.size()
                        )
                    );
                    return;
                }
            }

            try {
                final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
                final IndexShard indexShard = indexService.getShard(shardId.id());

                checkpointsByIndexOfThisNode.computeIfAbsent(shardId.getIndexName(), k -> {
                    long[] seqNumbers = new long[indexService.getIndexSettings().getNumberOfShards()];
                    Arrays.fill(seqNumbers, SequenceNumbers.UNASSIGNED_SEQ_NO);
                    return seqNumbers;
                });
                checkpointsByIndexOfThisNode.get(shardId.getIndexName())[shardId.getId()] = indexShard.seqNoStats().getGlobalCheckpoint();
                ++numProcessedShards;
            } catch (Exception e) {
                logger.atDebug()
                    .withThrowable(e)
                    .log("Failed to get checkpoint for shard [{}] and index [{}]", shardId.getId(), shardId.getIndexName());
                listener.onFailure(e);
                return;
            }
        }

        listener.onResponse(new Response(checkpointsByIndexOfThisNode));
    }
}
