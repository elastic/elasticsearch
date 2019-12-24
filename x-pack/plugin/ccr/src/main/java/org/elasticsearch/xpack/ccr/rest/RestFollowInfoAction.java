/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction;

public class RestFollowInfoAction extends BaseRestHandler {

    public RestFollowInfoAction(final RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_ccr/info", this);
    }

    @Override
    public String getName() {
        return "ccr_follower_info";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        final FollowInfoAction.Request request = new FollowInfoAction.Request();
        request.setFollowerIndices(Strings.splitStringByCommaToArray(restRequest.param("index")));
        return channel -> client.execute(FollowInfoAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

}
