/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;

import static org.elasticsearch.xpack.ccr.action.FollowExistingIndexAction.INSTANCE;
import static org.elasticsearch.xpack.ccr.action.FollowExistingIndexAction.Request;
import static org.elasticsearch.xpack.ccr.action.FollowExistingIndexAction.Response;

// TODO: change to confirm with API design
public class RestFollowExistingIndexAction extends BaseRestHandler {

    public RestFollowExistingIndexAction(Settings settings, RestController controller) {
        super(settings);
        // TODO: figure out why: '/{follow_index}/_xpack/ccr/_follow' path clashes with create index api.
        controller.registerHandler(RestRequest.Method.POST, "/_xpack/ccr/{follow_index}/_follow", this);
    }

    @Override
    public String getName() {
        return "xpack_ccr_follow_index_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Request request = new Request();
        request.setLeaderIndex(restRequest.param("leader_index"));
        request.setFollowIndex(restRequest.param("follow_index"));
        return channel -> client.execute(INSTANCE, request, new RestBuilderListener<Response>(channel) {
            @Override
            public RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, builder.startObject()
                        .endObject());

            }
        });
    }
}