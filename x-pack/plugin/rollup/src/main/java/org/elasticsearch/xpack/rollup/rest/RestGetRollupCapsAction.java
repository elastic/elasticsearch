/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.rollup.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.rollup.action.GetRollupCapsAction;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetRollupCapsAction extends BaseRestHandler {

    public static final ParseField ID = new ParseField("id");

    public RestGetRollupCapsAction(RestController controller) {
        controller.registerHandler(GET, "/_rollup/data/{id}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(ID.getPreferredName());
        GetRollupCapsAction.Request request = new GetRollupCapsAction.Request(id);

        return channel -> client.execute(GetRollupCapsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "get_rollup_caps";
    }

}
