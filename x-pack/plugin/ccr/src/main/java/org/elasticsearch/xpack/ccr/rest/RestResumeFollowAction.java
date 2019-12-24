/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.INSTANCE;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request;

public class RestResumeFollowAction extends BaseRestHandler {

    public RestResumeFollowAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_ccr/resume_follow", this);
    }

    @Override
    public String getName() {
        return "ccr_resume_follow_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Request request = createRequest(restRequest);
        return channel -> client.execute(INSTANCE, request, new RestToXContentListener<>(channel));
    }

    static Request createRequest(RestRequest restRequest) throws IOException {
        if (restRequest.hasContentOrSourceParam()) {
            try (XContentParser parser = restRequest.contentOrSourceParamParser()) {
                return Request.fromXContent(parser, restRequest.param("index"));
            }
        } else {
            Request request = new Request();
            request.setFollowerIndex(restRequest.param("index"));
            return request;
        }
    }
}
