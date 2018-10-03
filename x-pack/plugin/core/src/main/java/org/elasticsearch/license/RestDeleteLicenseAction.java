/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

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
    public RestDeleteLicenseAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(DELETE, URI_BASE + "/license", this);
    }

    @Override
    public String getName() {
        return "xpack_delete_license_action";
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
