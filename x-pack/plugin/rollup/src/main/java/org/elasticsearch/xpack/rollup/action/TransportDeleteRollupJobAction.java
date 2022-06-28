/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.rollup.action.DeleteRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;
import org.elasticsearch.xpack.rollup.job.RollupJobTask;

import java.util.List;

public class TransportDeleteRollupJobAction extends TransportTasksAction<
    RollupJobTask,
    DeleteRollupJobAction.Request,
    DeleteRollupJobAction.Response,
    DeleteRollupJobAction.Response> {

    @Inject
    public TransportDeleteRollupJobAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(
            DeleteRollupJobAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            DeleteRollupJobAction.Request::new,
            DeleteRollupJobAction.Response::new,
            DeleteRollupJobAction.Response::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void doExecute(Task task, DeleteRollupJobAction.Request request, ActionListener<DeleteRollupJobAction.Response> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();

        if (nodes.isLocalNodeElectedMaster()) {
            PersistentTasksCustomMetadata pTasksMeta = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
            if (pTasksMeta != null && pTasksMeta.getTask(request.getId()) != null) {
                super.doExecute(task, request, listener);
            } else {
                // If we couldn't find the job in the persistent task CS, it means it was deleted prior to this call,
                // no need to go looking for the allocated task
                listener.onFailure(new ResourceNotFoundException("the task with id [" + request.getId() + "] doesn't exist"));
            }

        } else {
            // Delegates DeleteJob to elected master node, so it becomes the coordinating node.
            // Non-master nodes may have a stale cluster state that shows jobs which are cancelled
            // on the master, which makes testing difficult.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException("no known master nodes"));
            } else {
                transportService.sendRequest(
                    nodes.getMasterNode(),
                    actionName,
                    request,
                    new ActionListenerResponseHandler<>(listener, DeleteRollupJobAction.Response::new)
                );
            }
        }
    }

    @Override
    protected void taskOperation(
        DeleteRollupJobAction.Request request,
        RollupJobTask jobTask,
        ActionListener<DeleteRollupJobAction.Response> listener
    ) {

        assert jobTask.getConfig().getId().equals(request.getId());
        IndexerState state = ((RollupJobStatus) jobTask.getStatus()).getIndexerState();
        if (state.equals(IndexerState.STOPPED)) {
            jobTask.onCancelled();
            listener.onResponse(new DeleteRollupJobAction.Response(true));
        } else {
            listener.onFailure(
                new IllegalStateException(
                    "Could not delete job ["
                        + request.getId()
                        + "] because "
                        + "indexer state is ["
                        + state
                        + "].  Job must be ["
                        + IndexerState.STOPPED
                        + "] before deletion."
                )
            );
        }
    }

    @Override
    protected DeleteRollupJobAction.Response newResponse(
        DeleteRollupJobAction.Request request,
        List<DeleteRollupJobAction.Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        // There should theoretically only be one task running the rollup job
        // If there are more, in production it should be ok as long as they are acknowledge shutting down.
        // But in testing we'd like to know there were more than one hence the assert
        assert tasks.size() + taskOperationFailures.size() == 1;
        boolean cancelled = tasks.size() > 0 && tasks.stream().allMatch(DeleteRollupJobAction.Response::isDeleted);
        return new DeleteRollupJobAction.Response(cancelled, taskOperationFailures, failedNodeExceptions);
    }

}
