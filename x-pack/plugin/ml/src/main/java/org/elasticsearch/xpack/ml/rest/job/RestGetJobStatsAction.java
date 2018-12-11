/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;

public class RestGetJobStatsAction extends BaseRestHandler {

    public RestGetJobStatsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.BASE_PATH
                + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_stats", this);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.V7_BASE_PATH
                + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_stats", this);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.BASE_PATH
                + "anomaly_detectors/_stats", this);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.V7_BASE_PATH
                + "anomaly_detectors/_stats", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_get_job_stats_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        if (Strings.isNullOrEmpty(jobId)) {
            jobId = MetaData.ALL;
        }
        GetJobsStatsAction.Request request = new GetJobsStatsAction.Request(jobId);
        request.setAllowNoJobs(restRequest.paramAsBoolean(GetJobsStatsAction.Request.ALLOW_NO_JOBS.getPreferredName(),
                request.allowNoJobs()));
        return channel -> client.execute(GetJobsStatsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
