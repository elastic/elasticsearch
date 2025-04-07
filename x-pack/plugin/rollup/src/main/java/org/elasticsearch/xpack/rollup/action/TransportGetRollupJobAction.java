/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.GetRollupJobsAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;
import org.elasticsearch.xpack.rollup.job.RollupJobTask;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.rollup.Rollup.DEPRECATION_KEY;
import static org.elasticsearch.xpack.rollup.Rollup.DEPRECATION_MESSAGE;

public class TransportGetRollupJobAction extends TransportTasksAction<
    RollupJobTask,
    GetRollupJobsAction.Request,
    GetRollupJobsAction.Response,
    GetRollupJobsAction.Response> {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(TransportGetRollupCapsAction.class);

    private final ProjectResolver projectResolver;

    @Inject
    public TransportGetRollupJobAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        super(
            GetRollupJobsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            GetRollupJobsAction.Request::new,
            GetRollupJobsAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task task, GetRollupJobsAction.Request request, ActionListener<GetRollupJobsAction.Response> listener) {
        DEPRECATION_LOGGER.warn(DeprecationCategory.API, DEPRECATION_KEY, DEPRECATION_MESSAGE);
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();

        if (nodes.isLocalNodeElectedMaster()) {
            final ProjectMetadata project = projectResolver.getProjectMetadata(state);
            if (stateHasRollupJobs(request, project)) {
                super.doExecute(task, request, listener);
            } else {
                // If we couldn't find the job in the persistent task CS, it means it was deleted prior to this GET
                // and we can just send an empty response, no need to go looking for the allocated task
                listener.onResponse(new GetRollupJobsAction.Response(Collections.emptyList()));
            }

        } else {
            // Delegates GetJobs to elected master node, so it becomes the coordinating node.
            // Non-master nodes may have a stale cluster state that shows jobs which are cancelled
            // on the master, which makes testing difficult.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException());
            } else {
                transportService.sendRequest(
                    nodes.getMasterNode(),
                    actionName,
                    request,
                    new ActionListenerResponseHandler<>(
                        listener,
                        GetRollupJobsAction.Response::new,
                        TransportResponseHandler.TRANSPORT_WORKER
                    )
                );
            }
        }
    }

    /**
     * Check to see if the PersistentTask's cluster state contains the rollup job(s) we are interested in
     */
    static boolean stateHasRollupJobs(GetRollupJobsAction.Request request, ProjectMetadata project) {
        boolean hasRollupJobs = false;
        PersistentTasksCustomMetadata pTasksMeta = project.custom(PersistentTasksCustomMetadata.TYPE);

        if (pTasksMeta != null) {
            // If the request was for _all rollup jobs, we need to look through the list of
            // persistent tasks and see if at least once has a RollupJob param
            if (request.getId().equals(Metadata.ALL)) {
                hasRollupJobs = pTasksMeta.tasks()
                    .stream()
                    .anyMatch(persistentTask -> persistentTask.getTaskName().equals(RollupField.TASK_NAME));

            } else if (pTasksMeta.getTask(request.getId()) != null) {
                // If we're looking for a single job, we can just check directly
                hasRollupJobs = true;
            }
        }
        return hasRollupJobs;
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        GetRollupJobsAction.Request request,
        RollupJobTask jobTask,
        ActionListener<GetRollupJobsAction.Response> listener
    ) {
        List<GetRollupJobsAction.JobWrapper> jobs = Collections.emptyList();

        assert jobTask.getConfig().getId().equals(request.getId()) || request.getId().equals(Metadata.ALL);

        // Little extra insurance, make sure we only return jobs that aren't cancelled
        if (jobTask.isCancelled() == false) {
            GetRollupJobsAction.JobWrapper wrapper = new GetRollupJobsAction.JobWrapper(
                jobTask.getConfig(),
                jobTask.getStats(),
                (RollupJobStatus) jobTask.getStatus()
            );
            jobs = Collections.singletonList(wrapper);
        }

        listener.onResponse(new GetRollupJobsAction.Response(jobs));
    }

    @Override
    protected GetRollupJobsAction.Response newResponse(
        GetRollupJobsAction.Request request,
        List<GetRollupJobsAction.Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        List<GetRollupJobsAction.JobWrapper> jobs = tasks.stream()
            .map(GetRollupJobsAction.Response::getJobs)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
        return new GetRollupJobsAction.Response(jobs, taskOperationFailures, failedNodeExceptions);
    }

}
