/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.filter;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

public class RestPutFilterAction extends BaseRestHandler {

    public RestPutFilterAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.PUT,
                MachineLearning.BASE_PATH + "filters/{" + MlFilter.ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.PUT,
                MachineLearning.V7_BASE_PATH + "filters/{" + MlFilter.ID.getPreferredName() + "}", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_put_filter_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String filterId = restRequest.param(MlFilter.ID.getPreferredName());
        XContentParser parser = restRequest.contentOrSourceParamParser();
        PutFilterAction.Request putFilterRequest = PutFilterAction.Request.parseRequest(filterId, parser);
        return channel -> client.execute(PutFilterAction.INSTANCE, putFilterRequest, new RestToXContentListener<>(channel));
    }

}
