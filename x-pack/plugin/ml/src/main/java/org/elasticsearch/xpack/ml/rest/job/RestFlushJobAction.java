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
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;

public class RestFlushJobAction extends BaseRestHandler {

    private final boolean DEFAULT_CALC_INTERIM = false;
    private final String DEFAULT_START = "";
    private final String DEFAULT_END = "";
    private final String DEFAULT_ADVANCE_TIME = "";
    private final String DEFAULT_SKIP_TIME = "";

    public RestFlushJobAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.BASE_PATH
                + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_flush", this);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.V7_BASE_PATH
                + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_flush", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_flush_job_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        final FlushJobAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            request = FlushJobAction.Request.parseRequest(jobId, parser);
        } else {
            request = new FlushJobAction.Request(restRequest.param(Job.ID.getPreferredName()));
            request.setCalcInterim(restRequest.paramAsBoolean(FlushJobAction.Request.CALC_INTERIM.getPreferredName(),
                    DEFAULT_CALC_INTERIM));
            request.setStart(restRequest.param(FlushJobAction.Request.START.getPreferredName(), DEFAULT_START));
            request.setEnd(restRequest.param(FlushJobAction.Request.END.getPreferredName(), DEFAULT_END));
            request.setAdvanceTime(restRequest.param(FlushJobAction.Request.ADVANCE_TIME.getPreferredName(), DEFAULT_ADVANCE_TIME));
            request.setSkipTime(restRequest.param(FlushJobAction.Request.SKIP_TIME.getPreferredName(), DEFAULT_SKIP_TIME));
        }

        return channel -> client.execute(FlushJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
