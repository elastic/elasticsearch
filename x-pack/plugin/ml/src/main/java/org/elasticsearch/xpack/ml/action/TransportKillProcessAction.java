/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.task.JobTask;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TransportKillProcessAction extends TransportTasksAction<
    JobTask,
    KillProcessAction.Request,
    KillProcessAction.Response,
    KillProcessAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportKillProcessAction.class);

    private final AnomalyDetectionAuditor auditor;

    @Inject
    public TransportKillProcessAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        AnomalyDetectionAuditor auditor
    ) {
        super(
            KillProcessAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            KillProcessAction.Request::new,
            KillProcessAction.Response::new,
            transportService.getThreadPool().executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
        );
        this.auditor = auditor;
    }

    @Override
    protected KillProcessAction.Response newResponse(
        KillProcessAction.Request request,
        List<KillProcessAction.Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        org.elasticsearch.ExceptionsHelper.rethrowAndSuppress(
            taskOperationFailures.stream().map(ExceptionsHelper::taskOperationFailureToStatusException).collect(Collectors.toList())
        );
        org.elasticsearch.ExceptionsHelper.rethrowAndSuppress(failedNodeExceptions);
        return new KillProcessAction.Response(true);
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        KillProcessAction.Request request,
        JobTask jobTask,
        ActionListener<KillProcessAction.Response> listener
    ) {
        logger.info("[{}] Killing job", jobTask.getJobId());
        auditor.info(jobTask.getJobId(), Messages.JOB_AUDIT_KILLING);
        try {
            jobTask.killJob("kill process (api)");
            listener.onResponse(new KillProcessAction.Response(true));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected void doExecute(Task task, KillProcessAction.Request request, ActionListener<KillProcessAction.Response> listener) {
        DiscoveryNodes nodes = clusterService.state().nodes();
        PersistentTasksCustomMetadata tasks = PersistentTasksCustomMetadata.get(clusterService.state().metadata().getDefaultProject());
        List<PersistentTasksCustomMetadata.PersistentTask<?>> jobTasks;
        if (Strings.isAllOrWildcard(request.getJobId())) {
            jobTasks = MlTasks.openJobTasks(tasks).stream().filter(t -> t.getExecutorNode() != null).collect(Collectors.toList());

        } else {
            PersistentTasksCustomMetadata.PersistentTask<?> jobTask = MlTasks.getJobTask(request.getJobId(), tasks);
            if (jobTask == null || jobTask.getExecutorNode() == null) {
                jobTasks = Collections.emptyList();
            } else {
                jobTasks = Collections.singletonList(jobTask);
            }
        }

        if (jobTasks.isEmpty()) {
            logger.debug("[{}] Cannot kill the process because job(s) are not open", request.getJobId());
            listener.onResponse(new KillProcessAction.Response(false));
            return;
        }
        if (jobTasks.stream().allMatch(t -> nodes.get(t.getExecutorNode()) == null)) {
            listener.onFailure(
                ExceptionsHelper.conflictStatusException(
                    "Cannot kill process for job {} as" + "executor node {} cannot be found",
                    request.getJobId(),
                    jobTasks.get(0).getExecutorNode()
                )
            );
            return;
        }
        request.setNodes(
            jobTasks.stream()
                .filter(t -> t.getExecutorNode() != null && nodes.get(t.getExecutorNode()) != null)
                .map(PersistentTasksCustomMetadata.PersistentTask::getExecutorNode)
                .toArray(String[]::new)
        );
        super.doExecute(task, request, listener);
    }

}
