/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestPostDataAction extends BaseRestHandler {

    private static final String DEFAULT_RESET_START = "";
    private static final String DEFAULT_RESET_END = "";

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestPostDataAction.class));

    public RestPostDataAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
            POST, MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_data", this,
            POST, MachineLearning.PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_data", deprecationLogger);
    }

    @Override
    public String getName() {
        return "ml_post_data_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PostDataAction.Request request = new PostDataAction.Request(restRequest.param(Job.ID.getPreferredName()));
        request.setResetStart(restRequest.param(PostDataAction.Request.RESET_START.getPreferredName(), DEFAULT_RESET_START));
        request.setResetEnd(restRequest.param(PostDataAction.Request.RESET_END.getPreferredName(), DEFAULT_RESET_END));
        request.setContent(restRequest.content(), restRequest.getXContentType());

        return channel -> client.execute(PostDataAction.INSTANCE, request, new RestStatusToXContentListener<>(channel));
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }
}
