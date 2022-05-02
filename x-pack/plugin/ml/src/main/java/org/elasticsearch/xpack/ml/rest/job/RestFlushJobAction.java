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
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

public class RestFlushJobAction extends BaseRestHandler {

    private static final boolean DEFAULT_CALC_INTERIM = false;
    private static final String DEFAULT_START = "";
    private static final String DEFAULT_END = "";
    private static final String DEFAULT_ADVANCE_TIME = "";
    private static final String DEFAULT_SKIP_TIME = "";

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/_flush")
                .replaces(POST, PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/_flush", RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "ml_flush_job_action";
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
            request.setCalcInterim(
                restRequest.paramAsBoolean(FlushJobAction.Request.CALC_INTERIM.getPreferredName(), DEFAULT_CALC_INTERIM)
            );
            request.setStart(restRequest.param(FlushJobAction.Request.START.getPreferredName(), DEFAULT_START));
            request.setEnd(restRequest.param(FlushJobAction.Request.END.getPreferredName(), DEFAULT_END));
            request.setAdvanceTime(restRequest.param(FlushJobAction.Request.ADVANCE_TIME.getPreferredName(), DEFAULT_ADVANCE_TIME));
            request.setSkipTime(restRequest.param(FlushJobAction.Request.SKIP_TIME.getPreferredName(), DEFAULT_SKIP_TIME));
        }

        return channel -> client.execute(FlushJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
