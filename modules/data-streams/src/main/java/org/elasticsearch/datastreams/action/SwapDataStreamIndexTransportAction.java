/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.SwapDataStreamIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

public class SwapDataStreamIndexTransportAction extends TransportMasterNodeAction<
    SwapDataStreamIndexAction.Request,
    SwapDataStreamIndexAction.Response> {
    private final MasterServiceTaskQueue<UpdateDataStreamTask> reindexDataStreamClusterStateUpdateTaskQueue;

    private static final SimpleBatchedExecutor<UpdateDataStreamTask, Void> REINDEX_DATA_STREAM_STATE_UPDATE_TASK_EXECUTOR =
        new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Void> executeTask(UpdateDataStreamTask task, ClusterState clusterState) throws Exception {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(UpdateDataStreamTask task, Void unused) {
                task.listener.onResponse(null);
            }
        };

    @Inject
    public SwapDataStreamIndexTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            SwapDataStreamIndexAction.NAME,
            true,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            SwapDataStreamIndexAction.Request::new,
            indexNameExpressionResolver,
            SwapDataStreamIndexAction.Response::new,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.reindexDataStreamClusterStateUpdateTaskQueue = clusterService.createTaskQueue(
            "reindex-data-stream-state-update",
            Priority.LOW,
            REINDEX_DATA_STREAM_STATE_UPDATE_TASK_EXECUTOR
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        SwapDataStreamIndexAction.Request request,
        ClusterState state,
        ActionListener<SwapDataStreamIndexAction.Response> listener
    ) throws Exception {
        reindexDataStreamClusterStateUpdateTaskQueue.submitTask(
            Strings.format(
                "Swapping indices in [%s]. Adding [%s] and removing[%s]",
                request.getDataStream(),
                request.getNewIndex(),
                request.getOldIndex()
            ),
            new UpdateDataStreamTask(
                ActionListener.wrap(unused -> listener.onResponse(new SwapDataStreamIndexAction.Response("")), listener::onFailure),
                request.getDataStream(),
                request.getOldIndex(),
                request.getNewIndex()
            ),
            null
        );
    }

    @Override
    protected ClusterBlockException checkBlock(SwapDataStreamIndexAction.Request request, ClusterState state) {
        return null;
    }

    static class UpdateDataStreamTask implements ClusterStateTaskListener {
        private final ActionListener<Void> listener;
        private final String dataStream;
        private final String oldIndex;
        private final String newIndex;

        UpdateDataStreamTask(ActionListener<Void> listener, String dataStream, String oldIndex, String newIndex) {
            this.listener = listener;
            this.dataStream = dataStream;
            this.oldIndex = oldIndex;
            this.newIndex = newIndex;
        }

        ClusterState execute(ClusterState currentState) throws Exception {
            Metadata currentMetadata = currentState.metadata();
            DataStream oldDataStream = currentMetadata.dataStreams().get(dataStream);
            List<Index> indicesWithoutOldIndex = oldDataStream.getIndices()
                .stream()
                .filter(index -> index.getName().equals(oldIndex) == false)
                .toList();
            List<Index> newIndices = new ArrayList<>(indicesWithoutOldIndex);
            newIndices.add(currentMetadata.index(newIndex).getIndex());
            DataStream newDataStream = oldDataStream.copy()
                .setBackingIndices(DataStream.DataStreamIndices.backingIndicesBuilder(newIndices).build())
                .build();
            Metadata metadata = Metadata.builder(currentMetadata).put(newDataStream).build();
            return ClusterState.builder(currentState).metadata(metadata).build();
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }
}
