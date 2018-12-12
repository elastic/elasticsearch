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
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutFilterAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestPutFilterAction.class));

    public RestPutFilterAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
            PUT, MachineLearning.BASE_PATH + "filters/{" + MlFilter.ID.getPreferredName() + "}", this,
            PUT, MachineLearning.PRE_V7_BASE_PATH + "filters/{" + MlFilter.ID.getPreferredName() + "}", deprecationLogger);
    }

    @Override
    public String getName() {
        return "ml_put_filter_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String filterId = restRequest.param(MlFilter.ID.getPreferredName());
        XContentParser parser = restRequest.contentOrSourceParamParser();
        PutFilterAction.Request putFilterRequest = PutFilterAction.Request.parseRequest(filterId, parser);
        return channel -> client.execute(PutFilterAction.INSTANCE, putFilterRequest, new RestToXContentListener<>(channel));
    }

}
