/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.license;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestPostStartBasicLicense extends XPackRestHandler {

    RestPostStartBasicLicense() {}

    @Override
    public List<Route> routes() {
        return emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return singletonList(new ReplacedRoute(POST, "/_license/start_basic", POST, URI_BASE + "/license/start_basic"));
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
        return "post_start_basic";
    }

}
