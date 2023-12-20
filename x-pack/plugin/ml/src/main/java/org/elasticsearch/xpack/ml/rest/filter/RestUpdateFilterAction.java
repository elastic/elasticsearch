/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.filter;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestUpdateFilterAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, BASE_PATH + "filters/{" + MlFilter.ID + "}/_update")
                .replaces(POST, PRE_V7_BASE_PATH + "filters/{" + MlFilter.ID + "}/_update", RestApiVersion.V_7)
                .build()
        );
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
