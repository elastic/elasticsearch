/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.utils.JobStateObserver;

/**
 * Service that interacts with a client to close jobs remotely.
 */
// Ideally this would sit in CloseJobAction.TransportAction, but we can't inject a client there as
// it would lead to cyclic dependency issue, so we isolate it here.
public class CloseJobService {

    private final Client client;
    private final JobStateObserver observer;

    public CloseJobService(Client client, ThreadPool threadPool, ClusterService clusterService) {
        this.client = client;
        this.observer = new JobStateObserver(threadPool, clusterService);
    }

    void closeJob(CloseJobAction.Request request, ActionListener<CloseJobAction.Response> listener) {
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setDetailed(true);
        listTasksRequest.setActions(OpenJobAction.NAME + "[c]");
        client.admin().cluster().listTasks(listTasksRequest, ActionListener.wrap(listTasksResponse -> {
            String expectedDescription = "job-" + request.getJobId();
            for (TaskInfo taskInfo : listTasksResponse.getTasks()) {
                if (expectedDescription.equals(taskInfo.getDescription())) {
                    CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
                    cancelTasksRequest.setTaskId(taskInfo.getTaskId());
                    client.admin().cluster().cancelTasks(cancelTasksRequest, ActionListener.wrap(cancelTaskResponse -> {
                        observer.waitForState(request.getJobId(), request.getTimeout(), JobState.CLOSED, e -> {
                            if (e == null) {
                                listener.onResponse(new CloseJobAction.Response(true));
                            } else {
                                listener.onFailure(e);
                            }
                        });
                    }, listener::onFailure));
                    return;
                }
            }
            listener.onFailure(new ResourceNotFoundException("task not found for job [" + request.getJobId() + "]"));
        }, listener::onFailure));
    }

}
