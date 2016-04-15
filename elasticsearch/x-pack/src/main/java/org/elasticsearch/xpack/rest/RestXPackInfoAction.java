/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rest;

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
import org.elasticsearch.xpack.action.XPackInfoResponse;

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
        client.prepareInfo().execute(new RestBuilderListener<XPackInfoResponse>(restChannel) {
            @Override
            public RestResponse buildResponse(XPackInfoResponse infoResponse, XContentBuilder builder) throws Exception {

                // we treat HEAD requests as simple pings to ensure that X-Pack is installed
                // we still execute the action as we want this request to be authorized
                if (request.method() == RestRequest.Method.HEAD) {
                    return new BytesRestResponse(OK);
                }

                builder.startObject();
                builder.field("build", infoResponse.getBuildInfo(), request);
                if (infoResponse.getLicenseInfo() != null) {
                    builder.field("license", infoResponse.getLicenseInfo(), request);
                } else {
                    builder.nullField("license");
                }
                builder.field("tagline", "You know, for X");
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }
}
