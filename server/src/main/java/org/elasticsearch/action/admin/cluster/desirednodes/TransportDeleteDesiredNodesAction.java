/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DesiredNodesMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportDeleteDesiredNodesAction extends TransportMasterNodeAction<AcknowledgedRequest.Plain, ActionResponse.Empty> {

    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>("cluster:admin/desired_nodes/delete");
    private final MasterServiceTaskQueue<DeleteDesiredNodesTask> taskQueue;

    @Inject
    public TransportDeleteDesiredNodesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            AcknowledgedRequest.Plain::new,
            in -> ActionResponse.Empty.INSTANCE,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.taskQueue = clusterService.createTaskQueue("delete-desired-nodes", Priority.HIGH, new DeleteDesiredNodesExecutor());
    }

    @Override
    protected void masterOperation(
        Task task,
        AcknowledgedRequest.Plain request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) throws Exception {
        taskQueue.submitTask("delete-desired-nodes", new DeleteDesiredNodesTask(listener), request.masterNodeTimeout());
    }

    @Override
    protected ClusterBlockException checkBlock(AcknowledgedRequest.Plain request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private record DeleteDesiredNodesTask(ActionListener<ActionResponse.Empty> listener) implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    private static class DeleteDesiredNodesExecutor extends SimpleBatchedExecutor<DeleteDesiredNodesTask, Void> {
        @Override
        public Tuple<ClusterState, Void> executeTask(DeleteDesiredNodesTask task, ClusterState clusterState) {
            return Tuple.tuple(clusterState, null);
        }

        @Override
        public void taskSucceeded(DeleteDesiredNodesTask task, Void unused) {
            task.listener().onResponse(ActionResponse.Empty.INSTANCE);
        }

        @Override
        public ClusterState afterBatchExecution(ClusterState clusterState, boolean clusterStateChanged) {
            return clusterState.copyAndUpdateMetadata(metadata -> metadata.removeCustom(DesiredNodesMetadata.TYPE));
        }
    }

    public static class Request extends AcknowledgedRequest<Request> {
        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            super(masterNodeTimeout, ackTimeout);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }
    }
}
