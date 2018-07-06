/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rest.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import java.io.IOException;
import java.util.EnumSet;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

public class RestXPackInfoAction extends XPackRestHandler {
    public RestXPackInfoAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(HEAD, URI_BASE, this);
        controller.registerHandler(GET, URI_BASE, this);
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
