/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.test.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * A handler to emit http warnings to help test the warnings and allowed_warnings feature
 */
public class EmitWarningEndpoint extends BaseRestHandler {

    @Override
    public List<RestHandler.Route> routes() {
        return List.of(new RestHandler.Route(GET, "/_rest_test/warning"));
    }

    @Override
    public String getName() {
        return "rest_test_warning";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        HeaderWarning.addWarning("This is a header warning");
        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, ""));
    }


}
