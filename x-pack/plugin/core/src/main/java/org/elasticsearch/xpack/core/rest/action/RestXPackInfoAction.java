/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.XPackInfoRequestBuilder;

import java.io.IOException;
import java.util.EnumSet;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

public class RestXPackInfoAction extends BaseRestHandler {
    public RestXPackInfoAction(RestController controller) {
        controller.registerHandler(HEAD, "/_xpack", this);
        controller.registerHandler(GET, "/_xpack", this);
    }

    @Override
    public String getName() {
        return "xpack_info_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        // we piggyback verbosity on "human" output
        boolean verbose = request.paramAsBoolean("human", true);

        EnumSet<XPackInfoRequest.Category> categories = XPackInfoRequest.Category
                .toSet(request.paramAsStringArray("categories", new String[] { "_all" }));
        return channel ->
                new XPackInfoRequestBuilder(client)
                        .setVerbose(verbose)
                        .setCategories(categories)
                        .execute(new RestToXContentListener<>(channel));
    }
}
