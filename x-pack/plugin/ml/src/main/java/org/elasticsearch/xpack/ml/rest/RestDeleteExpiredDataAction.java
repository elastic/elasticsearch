/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteExpiredDataAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // TODO: remove deprecated endpoint in 8.0.0
        return Collections.singletonList(
            new ReplacedRoute(DELETE, MachineLearning.BASE_PATH + "_delete_expired_data",
                DELETE, MachineLearning.PRE_V7_BASE_PATH + "_delete_expired_data")
        );
    }

    @Override
    public String getName() {
        return "ml_delete_expired_data_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        DeleteExpiredDataAction.Request request = restRequest.hasContent() ?
            DeleteExpiredDataAction.Request.PARSER.apply(restRequest.contentParser(), null) :
            new DeleteExpiredDataAction.Request();
        return channel -> client.execute(DeleteExpiredDataAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
