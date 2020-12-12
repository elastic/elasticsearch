/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.rollup.v2.RollupAction;
import org.elasticsearch.xpack.core.rollup.v2.RollupActionConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestRollupAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_rollup"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String index = restRequest.param("index");
        RollupActionConfig config = RollupActionConfig.fromXContent(restRequest.contentParser());
        RollupAction.Request request = new RollupAction.Request(index, config);
        return channel -> client.execute(RollupAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "rollup_action";
    }

}
