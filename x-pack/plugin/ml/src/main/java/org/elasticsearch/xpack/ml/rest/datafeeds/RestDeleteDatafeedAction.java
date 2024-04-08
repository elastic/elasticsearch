/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestDeleteDatafeedAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(DELETE, BASE_PATH + "datafeeds/{" + DatafeedConfig.ID + "}")
                .replaces(DELETE, PRE_V7_BASE_PATH + "datafeeds/{" + DatafeedConfig.ID + "}", RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "ml_delete_datafeed_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        DeleteDatafeedAction.Request request = new DeleteDatafeedAction.Request(datafeedId);
        if (restRequest.hasParam(DeleteDatafeedAction.Request.FORCE.getPreferredName())) {
            request.setForce(restRequest.paramAsBoolean(CloseJobAction.Request.FORCE.getPreferredName(), request.isForce()));
        }
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
        return channel -> client.execute(DeleteDatafeedAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
