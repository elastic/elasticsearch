/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.deprecation.DeprecationInfoAction.Request;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestDeprecationInfoAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, "/_migration/deprecations")
                .replaces(GET, "/_xpack/migration/deprecations", RestApiVersion.V_7).build(),
            Route.builder(GET, "/{index}/_migration/deprecations")
                .replaces(GET, "/{index}/_xpack/migration/deprecations", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "deprecation_info";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.method().equals(GET)) {
            return handleGet(request, client);
        } else {
            throw new IllegalArgumentException("illegal method [" + request.method() + "] for request [" + request.path() + "]");
        }
    }

    private RestChannelConsumer handleGet(final RestRequest request, NodeClient client) {
        Request infoRequest = new Request(Strings.splitStringByCommaToArray(request.param("index")));
        return channel -> client.execute(DeprecationInfoAction.INSTANCE, infoRequest, new RestToXContentListener<>(channel));
    }
}
