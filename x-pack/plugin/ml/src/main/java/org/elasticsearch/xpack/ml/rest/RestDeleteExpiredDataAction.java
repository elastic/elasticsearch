/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xpack.core.ml.job.config.Job.ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestDeleteExpiredDataAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(DELETE, BASE_PATH + "_delete_expired_data/{" + ID + "}"),
            new Route(DELETE, BASE_PATH + "_delete_expired_data")
        );
    }

    @Override
    public String getName() {
        return "ml_delete_expired_data_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());

        DeleteExpiredDataAction.Request request;
        if (restRequest.hasContent()) {
            try (var parser = restRequest.contentParser()) {
                request = DeleteExpiredDataAction.Request.parseRequest(jobId, parser);
            }
        } else {
            request = new DeleteExpiredDataAction.Request();
            request.setJobId(jobId);

            String perSecondParam = restRequest.param(DeleteExpiredDataAction.Request.REQUESTS_PER_SECOND.getPreferredName());
            if (perSecondParam != null) {
                try {
                    request.setRequestsPerSecond(Float.parseFloat(perSecondParam));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                        "Failed to parse float parameter ["
                            + DeleteExpiredDataAction.Request.REQUESTS_PER_SECOND.getPreferredName()
                            + "] with value ["
                            + perSecondParam
                            + "]",
                        e
                    );
                }
            }

            String timeoutParam = restRequest.param(DeleteExpiredDataAction.Request.TIMEOUT.getPreferredName());
            if (timeoutParam != null) {
                request.setTimeout(restRequest.paramAsTime(timeoutParam, null));
            }
        }

        return channel -> client.execute(DeleteExpiredDataAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
