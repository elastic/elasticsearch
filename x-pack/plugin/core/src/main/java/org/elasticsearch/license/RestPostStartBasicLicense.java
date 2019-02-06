/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestPostStartBasicLicense extends XPackRestHandler {

    RestPostStartBasicLicense(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, URI_BASE + "/license/start_basic", this);
        controller.registerHandler(POST, "/_license/start_basic", this);
    }

    @Override
    protected RestChannelConsumer doPrepareRequest(RestRequest request, XPackClient client) throws IOException {
        PostStartBasicRequest startBasicRequest = new PostStartBasicRequest();
        startBasicRequest.acknowledge(request.paramAsBoolean("acknowledge", false));
        startBasicRequest.timeout(request.paramAsTime("timeout", startBasicRequest.timeout()));
        startBasicRequest.masterNodeTimeout(request.paramAsTime("master_timeout", startBasicRequest.masterNodeTimeout()));
        return channel -> client.licensing().postStartBasic(startBasicRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "xpack_start_basic_action";
    }
}
