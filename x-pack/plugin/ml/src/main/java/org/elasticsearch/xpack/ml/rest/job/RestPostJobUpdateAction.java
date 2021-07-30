/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

public class RestPostJobUpdateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/_update")
        );
    }

    @Override
    public String getName() {
        return "ml_post_job_update_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        XContentParser parser = restRequest.contentParser();
        UpdateJobAction.Request updateJobRequest = UpdateJobAction.Request.parseRequest(jobId, parser);
        updateJobRequest.timeout(restRequest.paramAsTime("timeout", updateJobRequest.timeout()));
        updateJobRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", updateJobRequest.masterNodeTimeout()));

        return channel -> client.execute(UpdateJobAction.INSTANCE, updateJobRequest, new RestToXContentListener<>(channel));
    }
}
