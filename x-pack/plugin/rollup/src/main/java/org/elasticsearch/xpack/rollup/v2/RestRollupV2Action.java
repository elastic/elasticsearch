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
import org.elasticsearch.xpack.core.rollup.v2.RollupV2Action;
import org.elasticsearch.xpack.core.rollup.v2.RollupV2Config;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestRollupV2Action extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_rollup_vtwo/{index}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String index = restRequest.param("index");
        RollupV2Config job = RollupV2Config.fromXContent(restRequest.contentParser(), index);
        RollupV2Action.Request request = new RollupV2Action.Request(job);
        return channel -> client.execute(RollupV2Action.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "rollupV2";
    }

}
