/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.transform.action.ScheduleNowTransformAction;
import org.elasticsearch.xpack.core.transform.action.ScheduleNowTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.ScheduleNowTransformAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.TransformTask;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;

import java.util.List;

import static org.elasticsearch.core.Strings.format;

public class TransportScheduleNowTransformAction extends TransportTasksAction<TransformTask, Request, Response, Response> {

    private static final Logger logger = LogManager.getLogger(TransportScheduleNowTransformAction.class);
    private final TransformConfigManager transformConfigManager;
    private final TransformScheduler transformScheduler;

    @Inject
    public TransportScheduleNowTransformAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        TransformServices transformServices
    ) {
        super(
            ScheduleNowTransformAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        this.transformConfigManager = transformServices.configManager();
        this.transformScheduler = transformServices.scheduler();
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState clusterState = clusterService.state();
        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);

        ActionListener<TransformConfig> getTransformListener = ActionListener.wrap(unusedConfig -> {
            PersistentTasksCustomMetadata.PersistentTask<?> transformTask = TransformTask.getTransformTask(request.getId(), clusterState);

            // to send a request to schedule now the transform at runtime, several requirements must be met:
            // - transform must be running, meaning a task exists
            // - transform is not failed (stopped transforms do not have a task)
            if (transformTask != null
                && transformTask.isAssigned()
                && transformTask.getState() instanceof TransformState
                && ((TransformState) transformTask.getState()).getTaskState() != TransformTaskState.FAILED) {

                ActionListener<Response> taskScheduleNowListener = ActionListener.wrap(listener::onResponse, e -> {
                    // benign: A transform might have been stopped meanwhile, this is not a problem
                    if (e instanceof TransformTaskDisappearedDuringScheduleNowException) {
                        logger.debug(() -> format("[%s] transform task disappeared during schedule_now, ignoring.", request.getId()), e);
                        listener.onResponse(Response.TRUE);
                        return;
                    }
                    if (e instanceof TransformTaskScheduleNowException) {
                        logger.warn(() -> format("[%s] failed to schedule now the running transform.", request.getId()), e);
                        listener.onResponse(Response.TRUE);
                        return;
                    }
                    listener.onFailure(e);
                });
                request.setNodes(transformTask.getExecutorNode());
                super.doExecute(task, request, taskScheduleNowListener);
            } else {
                listener.onResponse(Response.TRUE);
            }
        }, listener::onFailure);

        // <1> Get the config to verify it exists and is valid
        transformConfigManager.getTransformConfiguration(request.getId(), getTransformListener);
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        Request request,
        TransformTask transformTask,
        ActionListener<Response> listener
    ) {
        transformScheduler.scheduleNow(request.getId());
        listener.onResponse(Response.TRUE);
    }

    @Override
    protected Response newResponse(
        Request request,
        List<Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        if (tasks.isEmpty()) {
            if (taskOperationFailures.isEmpty() == false) {
                throw new TransformTaskScheduleNowException(
                    "Failed to schedule now the running transform due to task operation failure.",
                    taskOperationFailures.get(0).getCause()
                );
            } else if (failedNodeExceptions.isEmpty() == false) {
                FailedNodeException failedNodeException = failedNodeExceptions.get(0);
                Throwable failedNodeExceptionCause = ExceptionsHelper.unwrapCause(failedNodeException.getCause());
                if (failedNodeExceptionCause instanceof ActionNotFoundTransportException) {
                    throw (ActionNotFoundTransportException) failedNodeExceptionCause;
                }
                throw new TransformTaskScheduleNowException(
                    "Failed to schedule now the running transform due to failed node exception.",
                    failedNodeExceptions.get(0)
                );
            } else {
                throw new TransformTaskDisappearedDuringScheduleNowException(
                    "Could not schedule now the running transform as it has been stopped."
                );
            }
        }
        return tasks.get(0);
    }

    private static class TransformTaskScheduleNowException extends ElasticsearchException {
        TransformTaskScheduleNowException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }

    private static class TransformTaskDisappearedDuringScheduleNowException extends ElasticsearchException {
        TransformTaskDisappearedDuringScheduleNowException(String msg) {
            super(msg);
        }
    }
}
