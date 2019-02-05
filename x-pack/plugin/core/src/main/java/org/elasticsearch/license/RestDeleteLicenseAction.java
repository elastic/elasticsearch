/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.license;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.license.DeleteLicenseRequest;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteLicenseAction extends XPackRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestDeleteLicenseAction.class));

    RestDeleteLicenseAction(Settings settings, RestController controller) {
        super(settings);
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                DELETE, "/_license", this,
                DELETE, URI_BASE + "/license", deprecationLogger);
    }

    @Override
    public String getName() {
        return "delete_license";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(final RestRequest request, final XPackClient client) throws IOException {
        DeleteLicenseRequest deleteLicenseRequest = new DeleteLicenseRequest();
        deleteLicenseRequest.timeout(request.paramAsTime("timeout", deleteLicenseRequest.timeout()));
        deleteLicenseRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteLicenseRequest.masterNodeTimeout()));

        return channel -> client.es().admin().cluster().execute(DeleteLicenseAction.INSTANCE, deleteLicenseRequest,
                new RestToXContentListener<>(channel));
    }
}
