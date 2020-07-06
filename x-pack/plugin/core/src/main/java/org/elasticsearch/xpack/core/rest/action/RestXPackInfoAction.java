/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.XPackInfoRequestBuilder;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

public class RestXPackInfoAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestXPackInfoAction.class);

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_xpack"),
            new Route(HEAD, "/_xpack"));
    }

    @Override
    public String getName() {
        return "xpack_info_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        // we piggyback verbosity on "human" output
        boolean verbose = request.paramAsBoolean("human", true);

        // In 7.x, there was an opt-in flag to show "enterprise" licenses. In 8.0 the flag is deprecated and can only be true
        // TODO Remove this from 9.0
        if (request.hasParam("accept_enterprise")) {
            deprecationLogger.deprecate("get_license_accept_enterprise",
                "Including [accept_enterprise] in get license requests is deprecated." +
                    " The parameter will be removed in the next major version");
            if (request.paramAsBoolean("accept_enterprise", true) == false) {
                throw new IllegalArgumentException("The [accept_enterprise] parameters may not be false");
            }
        }

        EnumSet<XPackInfoRequest.Category> categories = XPackInfoRequest.Category
                .toSet(request.paramAsStringArray("categories", new String[] { "_all" }));
        return channel ->
                new XPackInfoRequestBuilder(client)
                        .setVerbose(verbose)
                        .setCategories(categories)
                        .execute(new RestToXContentListener<>(channel));
    }
}
