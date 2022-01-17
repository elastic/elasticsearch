/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

public class RestDeleteJobAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(DELETE, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}")
                .replaces(DELETE, PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}", RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "ml_delete_job_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        DeleteJobAction.Request deleteJobRequest = new DeleteJobAction.Request(restRequest.param(Job.ID.getPreferredName()));
        deleteJobRequest.setForce(restRequest.paramAsBoolean(CloseJobAction.Request.FORCE.getPreferredName(), deleteJobRequest.isForce()));
        deleteJobRequest.timeout(restRequest.paramAsTime("timeout", deleteJobRequest.timeout()));
        deleteJobRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", deleteJobRequest.masterNodeTimeout()));

        if (restRequest.paramAsBoolean("wait_for_completion", true)) {
            return channel -> client.execute(DeleteJobAction.INSTANCE, deleteJobRequest, new RestToXContentListener<>(channel));
        } else {
            deleteJobRequest.setShouldStoreResult(true);

            Task task = client.executeLocally(DeleteJobAction.INSTANCE, deleteJobRequest, nullTaskListener());
            // Send task description id instead of waiting for the message
            return channel -> {
                try (XContentBuilder builder = channel.newBuilder()) {
                    builder.startObject();
                    builder.field("task", client.getLocalNodeId() + ":" + task.getId());
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                }
            };
        }
    }

    // We do not want to log anything due to a delete action
    // The response or error will be returned to the client when called synchronously
    // or it will be stored in the task result when called asynchronously
    private static <T> TaskListener<T> nullTaskListener() {
        return new TaskListener<T>() {
            @Override
            public void onResponse(Task task, T o) {}

            @Override
            public void onFailure(Task task, Exception e) {}
        };
    }
}
