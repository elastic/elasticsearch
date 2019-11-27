/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.license;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestPostStartBasicLicense extends BaseRestHandler {

    RestPostStartBasicLicense(RestController controller) {
        controller.registerHandler(POST, "/_license/start_basic", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        PostStartBasicRequest startBasicRequest = new PostStartBasicRequest();
        startBasicRequest.acknowledge(request.paramAsBoolean("acknowledge", false));
        startBasicRequest.timeout(request.paramAsTime("timeout", startBasicRequest.timeout()));
        startBasicRequest.masterNodeTimeout(request.paramAsTime("master_timeout", startBasicRequest.masterNodeTimeout()));
        return channel -> client.execute(PostStartBasicAction.INSTANCE, startBasicRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "post_start_basic";
    }

}
