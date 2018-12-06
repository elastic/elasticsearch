/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutLicenseAction extends XPackRestHandler {
    private static final Logger logger = LogManager.getLogger(RestPutLicenseAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public RestPutLicenseAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, URI_BASE + "/license", this);
        controller.registerHandler(POST, "/_license", this);
        controller.registerHandler(PUT, URI_BASE + "/license", this);
        controller.registerHandler(PUT, "/_license", this);

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
    public String getName() {
        return "xpack_put_license_action";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(final RestRequest request, final XPackClient client) throws IOException {
        if (request.hasContent() == false) {
            throw new IllegalArgumentException("The license must be provided in the request body");
        }
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest();
        putLicenseRequest.license(request.content(), request.getXContentType());
        putLicenseRequest.acknowledge(request.paramAsBoolean("acknowledge", false));
        putLicenseRequest.timeout(request.paramAsTime("timeout", putLicenseRequest.timeout()));
        putLicenseRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putLicenseRequest.masterNodeTimeout()));

        if ("basic".equals(putLicenseRequest.license().type())) {
            throw new IllegalArgumentException("Installing basic licenses is no longer allowed. Use the POST " +
                    "/_xpack/license/start_basic API to install a basic license that does not expire.");
        }

        return channel -> client.es().admin().cluster().execute(PutLicenseAction.INSTANCE, putLicenseRequest,
                new RestToXContentListener<>(channel));
    }
}
