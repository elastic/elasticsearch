/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestPostJobUpdateAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestPostJobUpdateAction.class));

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // TODO: remove deprecated endpoint in 8.0.0
        return Collections.singletonList(
            new ReplacedRoute(POST, MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_update",
                POST, MachineLearning.PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_update",
                deprecationLogger)
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
