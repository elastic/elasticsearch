/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.deprecation.DeprecationInfoAction.Request;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestDeprecationInfoAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_CAPABILITIES = Set.of("data_streams", "ilm_policies", "templates");

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_migration/deprecations"), new Route(GET, "/{index}/_migration/deprecations"));
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

    private static RestChannelConsumer handleGet(final RestRequest request, NodeClient client) {
        final var infoRequest = new Request(
            RestUtils.getMasterNodeTimeout(request),
            Strings.splitStringByCommaToArray(request.param("index"))
        );
        return channel -> client.execute(DeprecationInfoAction.INSTANCE, infoRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return SUPPORTED_CAPABILITIES;
    }
}
