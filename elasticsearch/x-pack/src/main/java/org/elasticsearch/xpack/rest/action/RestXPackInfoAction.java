/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.xpack.XPackClient;
import org.elasticsearch.xpack.action.XPackInfoRequest;
import org.elasticsearch.xpack.action.XPackInfoResponse;
import org.elasticsearch.xpack.rest.XPackRestHandler;

import java.util.EnumSet;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestXPackInfoAction extends XPackRestHandler {

    @Inject
    public RestXPackInfoAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(HEAD, URI_BASE, this);
        controller.registerHandler(GET, URI_BASE, this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel restChannel, XPackClient client) throws Exception {

        // we piggyback verbosity on "human" output
        boolean verbose = request.paramAsBoolean("human", true);

        EnumSet<XPackInfoRequest.Category> categories = XPackInfoRequest.Category
                .toSet(request.paramAsStringArray("categories", new String[] { "_all" }));
        client.prepareInfo().setVerbose(verbose).setCategories(categories).execute(new RestBuilderListener<XPackInfoResponse>(restChannel) {
            @Override
            public RestResponse buildResponse(XPackInfoResponse infoResponse, XContentBuilder builder) throws Exception {

                // we treat HEAD requests as simple pings to ensure that X-Pack is installed
                // we still execute the action as we want this request to be authorized
                if (request.method() == RestRequest.Method.HEAD) {
                    return new BytesRestResponse(OK);
                }

                builder.startObject();

                if (infoResponse.getBuildInfo() != null) {
                    builder.field("build", infoResponse.getBuildInfo(), request);
                }

                if (infoResponse.getLicenseInfo() != null) {
                    builder.field("license", infoResponse.getLicenseInfo(), request);
                } else if (categories.contains(XPackInfoRequest.Category.LICENSE)) {
                    // if the user requested the license info, and there is no license, we should send
                    // back an explicit null value (indicating there is no license). This is different
                    // than not adding the license info at all
                    builder.nullField("license");
                }

                if (infoResponse.getFeatureSetsInfo() != null) {
                    builder.field("features", infoResponse.getFeatureSetsInfo(), request);
                }

                if (verbose) {
                    builder.field("tagline", "You know, for X");
                }

                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }
}
