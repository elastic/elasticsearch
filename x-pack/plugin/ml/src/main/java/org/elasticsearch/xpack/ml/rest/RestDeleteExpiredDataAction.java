/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

public class RestDeleteExpiredDataAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(DELETE, BASE_PATH + "_delete_expired_data/{" + Job.ID + "}")
                .replaces(DELETE, PRE_V7_BASE_PATH + "_delete_expired_data/{" + Job.ID + "}", RestApiVersion.V_7)
                .build(),
            Route.builder(DELETE, BASE_PATH + "_delete_expired_data")
                .replaces(DELETE, PRE_V7_BASE_PATH + "_delete_expired_data", RestApiVersion.V_7)
                .build()
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
            request = DeleteExpiredDataAction.Request.parseRequest(jobId, restRequest.contentParser());
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
