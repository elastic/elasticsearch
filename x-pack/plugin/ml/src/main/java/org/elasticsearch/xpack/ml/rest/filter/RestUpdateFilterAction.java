/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.filter;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestUpdateFilterAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestUpdateFilterAction.class));

    public RestUpdateFilterAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
            POST, MachineLearning.BASE_PATH + "filters/{" + MlFilter.ID.getPreferredName() + "}/_update", this,
            POST, MachineLearning.PRE_V7_BASE_PATH + "filters/{" + MlFilter.ID.getPreferredName() + "}/_update", deprecationLogger);
    }

    @Override
    public String getName() {
        return "ml_update_filter_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String filterId = restRequest.param(MlFilter.ID.getPreferredName());
        XContentParser parser = restRequest.contentOrSourceParamParser();
        UpdateFilterAction.Request putFilterRequest = UpdateFilterAction.Request.parseRequest(filterId, parser);
        return channel -> client.execute(UpdateFilterAction.INSTANCE, putFilterRequest, new RestToXContentListener<>(channel));
    }
}
