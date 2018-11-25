/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.featureindexbuilder.FeatureIndexBuilder;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.GetDataFrameJobsAction.Request;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.GetDataFrameJobsAction.Response;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobConfig;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobTask;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TransportGetDataFrameJobsAction extends
        TransportTasksAction<FeatureIndexBuilderJobTask,
        GetDataFrameJobsAction.Request,
        GetDataFrameJobsAction.Response,
        GetDataFrameJobsAction.Response> {

    @Inject
    public TransportGetDataFrameJobsAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(GetDataFrameJobsAction.NAME, clusterService, transportService, actionFilters, GetDataFrameJobsAction.Request::new,
                GetDataFrameJobsAction.Response::new, ThreadPool.Names.SAME);
    }

    @Override
    protected Response newResponse(Request request, List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions) {
        List<FeatureIndexBuilderJobConfig> configs = tasks.stream().map(GetDataFrameJobsAction.Response::getJobConfigurations)
                .flatMap(Collection::stream).collect(Collectors.toList());
        return new Response(configs, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected Response readTaskResponse(StreamInput in) throws IOException {
        return new Response(in);
    }

    @Override
    protected void taskOperation(Request request, FeatureIndexBuilderJobTask task, ActionListener<Response> listener) {
        List<FeatureIndexBuilderJobConfig> configs = Collections.emptyList();

        assert task.getConfig().getId().equals(request.getId()) || request.getId().equals(MetaData.ALL);

        // Little extra insurance, make sure we only return jobs that aren't cancelled
        if (task.isCancelled() == false) {
            configs = Collections.singletonList(task.getConfig());
        }

        listener.onResponse(new Response(configs));
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();

        if (nodes.isLocalNodeElectedMaster()) {
            if (stateHasDataFrameJobs(request, state)) {
                super.doExecute(task, request, listener);
            } else {
                // If we couldn't find the job in the persistent task CS, it means it was deleted prior to this GET
                // and we can just send an empty response, no need to go looking for the allocated task
                listener.onResponse(new Response(Collections.emptyList()));
            }

        } else {
            // Delegates GetJobs to elected master node, so it becomes the coordinating node.
            // Non-master nodes may have a stale cluster state that shows jobs which are cancelled
            // on the master, which makes testing difficult.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException("no known master nodes"));
            } else {
                transportService.sendRequest(nodes.getMasterNode(), actionName, request,
                        new ActionListenerResponseHandler<>(listener, Response::new));
            }
        }
    }

    /**
     * Check to see if the PersistentTask's cluster state contains the job(s) we are interested in
     */
    static boolean stateHasDataFrameJobs(Request request, ClusterState state) {
        boolean hasJobs = false;
        PersistentTasksCustomMetaData pTasksMeta = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        if (pTasksMeta != null) {
            // If the request was for _all jobs, we need to look through the list of
            // persistent tasks and see if at least once has a DataFrameJob param
            if (request.getId().equals(MetaData.ALL)) {
                hasJobs = pTasksMeta.tasks()
                        .stream()
                        .anyMatch(persistentTask -> persistentTask.getTaskName().equals(FeatureIndexBuilder.TASK_NAME));

            } else if (pTasksMeta.getTask(request.getId()) != null) {
                // If we're looking for a single job, we can just check directly
                hasJobs = true;
            }
        }
        return hasJobs;
    }

}
