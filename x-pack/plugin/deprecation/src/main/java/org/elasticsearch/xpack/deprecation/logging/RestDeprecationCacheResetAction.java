/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation.logging;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.EmptyResponseListener;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.action.EmptyResponseListener.PLAIN_TEXT_EMPTY_RESPONSE_CAPABILITY_NAME;

public class RestDeprecationCacheResetAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_logging/deprecation_cache"));
    }

    @Override
    public String getName() {
        return "reset_deprecation_cache";
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(PLAIN_TEXT_EMPTY_RESPONSE_CAPABILITY_NAME);
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        DeprecationCacheResetAction.Request resetRequest = new DeprecationCacheResetAction.Request();
        return channel -> client.execute(
            DeprecationCacheResetAction.INSTANCE,
            resetRequest,
            new EmptyResponseListener(channel).map(ignored -> ActionResponse.Empty.INSTANCE)
        );
    }
}
