/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.rest;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.plugin.action.put.PutLicenseAction;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.xpack.XPackClient;
import org.elasticsearch.xpack.rest.XPackRestHandler;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutLicenseAction extends XPackRestHandler {

    @Inject
    public RestPutLicenseAction(Settings settings, RestController controller) {
        super(settings);
        // @deprecated Remove deprecations in 6.0
        controller.registerWithDeprecatedHandler(POST, URI_BASE + "/license", this,
                                                 POST, "/_license", deprecationLogger);
        controller.registerWithDeprecatedHandler(PUT, URI_BASE + "/license", this,
                                                 PUT, "/_license", deprecationLogger);

        // Remove _licenses support entirely in 6.0
        controller.registerAsDeprecatedHandler(POST, "/_licenses", this,
                                               "[POST /_licenses] is deprecated! Use " +
                                               "[POST /_xpack/license] instead.",
                                               deprecationLogger);
        controller.registerAsDeprecatedHandler(PUT, "/_licenses", this,
                                               "[PUT /_licenses] is deprecated! Use " +
                                               "[PUT /_xpack/license] instead.",
                                               deprecationLogger);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final XPackClient client) {
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest();
        putLicenseRequest.license(request.content().utf8ToString());
        putLicenseRequest.acknowledge(request.paramAsBoolean("acknowledge", false));
        client.es().admin().cluster().execute(PutLicenseAction.INSTANCE, putLicenseRequest,
                new RestBuilderListener<PutLicenseResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(PutLicenseResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject();
                        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
                        builder.endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
    }
}
