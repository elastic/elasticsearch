/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.shield.ShieldBuild;
import org.elasticsearch.shield.ShieldVersion;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestShieldInfoAction extends BaseRestHandler {

    @Inject
    public RestShieldInfoAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_shield", this);
        controller.registerHandler(HEAD, "/_shield", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        if (request.method() == RestRequest.Method.HEAD) {
            channel.sendResponse(new BytesRestResponse(OK));
            return;
        }

        XContentBuilder builder = channel.newBuilder();

        // Default to pretty printing, but allow ?pretty=false to disable
        if (!request.hasParam("pretty")) {
            builder.prettyPrint().lfAtEnd();
        }

        builder.startObject();
        if (settings.get("name") != null) {
            builder.field("name", settings.get("name"));
        }
        builder.startObject("version")
                .field("build_hash", ShieldBuild.CURRENT.hash())
                .field("build_timestamp", ShieldBuild.CURRENT.timestamp())
                .field("build_version", ShieldVersion.CURRENT.toString())
                .endObject();
        builder.field("tagline", "You know, for security");
        builder.endObject();

        channel.sendResponse(new BytesRestResponse(OK, builder));
    }
}
