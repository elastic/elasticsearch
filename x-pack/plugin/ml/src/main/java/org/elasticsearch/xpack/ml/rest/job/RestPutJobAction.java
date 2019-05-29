/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;

public class RestPutJobAction extends BaseRestHandler {

    public RestPutJobAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.PUT,
                MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.PUT,
                MachineLearning.V7_BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_put_job_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        XContentParser parser = restRequest.contentParser();
        PutJobAction.Request putJobRequest = PutJobAction.Request.parseRequest(jobId, parser);
        putJobRequest.timeout(restRequest.paramAsTime("timeout", putJobRequest.timeout()));
        putJobRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", putJobRequest.masterNodeTimeout()));

        return channel -> client.execute(PutJobAction.INSTANCE, putJobRequest, new RestToXContentListener<>(channel));
    }

}
