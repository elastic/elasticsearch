/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.support.local.TransportLocalClusterStateAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestGetStatusAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_ilm/status"));
    }

    @Override
    public String getName() {
        return "ilm_get_operation_mode_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        final var request = new GetStatusAction.Request(getMasterNodeTimeout(restRequest));
        if (restRequest.hasParam("timeout")) {
            // Consume this param just for validation when in BWC mode.
            final var timeout = restRequest.paramAsTime("timeout", null);
            if (restRequest.getRestApiVersion() != RestApiVersion.V_8) {
                DeprecationLogger.getLogger(TransportLocalClusterStateAction.class)
                    .critical(
                        DeprecationCategory.API,
                        "TransportLocalClusterStateAction-timeout-parameter",
                        "the [?timeout] query parameter to this API has no effect, is now deprecated, "
                            + "and will be removed in a future version"
                    );
            }

        }
        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            GetStatusAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel)
        );
    }
}
