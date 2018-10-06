/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ccr.action.CreateAndFollowIndexAction.INSTANCE;
import static org.elasticsearch.xpack.core.ccr.action.CreateAndFollowIndexAction.Request;

public class RestCreateAndFollowIndexAction extends BaseRestHandler {

    public RestCreateAndFollowIndexAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_ccr/create_and_follow", this);
    }

    @Override
    public String getName() {
        return "ccr_create_and_follow_index_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Request request = new Request(RestFollowIndexAction.createRequest(restRequest));
        return channel -> client.execute(INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
