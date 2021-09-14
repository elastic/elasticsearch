/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.dataframe;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

public class RestEvaluateDataFrameAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, BASE_PATH + "data_frame/_evaluate"));
    }

    @Override
    public String getName() {
        return "ml_evaluate_data_frame_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        EvaluateDataFrameAction.Request request = EvaluateDataFrameAction.Request.parseRequest(restRequest.contentOrSourceParamParser());
        return channel -> client.execute(EvaluateDataFrameAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
