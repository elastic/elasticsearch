/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rest.action;

import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

public class RestXPackInfoAction extends XPackRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_xpack"),
            new Route(HEAD, "/_xpack")));
    }

    @Override
    public String getName() {
        return "xpack_info_action";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(RestRequest request, XPackClient client) throws IOException {

        // we piggyback verbosity on "human" output
        boolean verbose = request.paramAsBoolean("human", true);

        EnumSet<XPackInfoRequest.Category> categories = XPackInfoRequest.Category
                .toSet(request.paramAsStringArray("categories", new String[] { "_all" }));
        return channel ->
                client.prepareInfo()
                        .setVerbose(verbose)
                        .setCategories(categories)
                        .execute(new RestToXContentListener<>(channel));
    }
}
