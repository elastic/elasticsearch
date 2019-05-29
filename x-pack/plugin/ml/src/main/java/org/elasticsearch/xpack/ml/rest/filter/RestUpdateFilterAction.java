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
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

public class RestUpdateFilterAction extends BaseRestHandler {

    public RestUpdateFilterAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST,
                MachineLearning.BASE_PATH + "filters/{" + MlFilter.ID.getPreferredName() + "}/_update", this);
        controller.registerHandler(RestRequest.Method.POST,
                MachineLearning.V7_BASE_PATH + "filters/{" + MlFilter.ID.getPreferredName() + "}/_update", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_update_filter_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String filterId = restRequest.param(MlFilter.ID.getPreferredName());
        XContentParser parser = restRequest.contentOrSourceParamParser();
        UpdateFilterAction.Request putFilterRequest = UpdateFilterAction.Request.parseRequest(filterId, parser);
        return channel -> client.execute(UpdateFilterAction.INSTANCE, putFilterRequest, new RestToXContentListener<>(channel));
    }
}
