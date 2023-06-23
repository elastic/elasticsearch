/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.action.ResetJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestResetJobAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/_reset"));
    }

    @Override
    public String getName() {
        return "ml_reset_job_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        ResetJobAction.Request request = new ResetJobAction.Request(restRequest.param(Job.ID.getPreferredName()));
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
        request.setDeleteUserAnnotations(restRequest.paramAsBoolean("delete_user_annotations", false));

        if (restRequest.paramAsBoolean("wait_for_completion", true)) {
            return channel -> client.execute(ResetJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
        } else {
            request.setShouldStoreResult(true);
            Task task = client.executeLocally(
                ResetJobAction.INSTANCE,
                request,
                /*
                 * We do not want to log anything due to a delete action. The response or error will be returned to the client when called
                 * synchronously or it will be stored in the task result when called asynchronously.
                 */
                ActionListener.noop()
            );
            return channel -> {
                try (XContentBuilder builder = channel.newBuilder()) {
                    builder.startObject();
                    builder.field("task", client.getLocalNodeId() + ":" + task.getId());
                    builder.endObject();
                    channel.sendResponse(new RestResponse(RestStatus.OK, builder));
                }
            };
        }
    }
}
