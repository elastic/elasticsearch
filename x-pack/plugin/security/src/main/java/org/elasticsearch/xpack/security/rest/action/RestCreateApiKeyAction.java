/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

public final class RestCreateApiKeyAction extends SecurityBaseRestHandler {

    /**
     * @param settings     the node's settings
     * @param licenseState the license state that will be used to determine if security is licensed
     */
    protected RestCreateApiKeyAction(Settings settings, XPackLicenseState licenseState, RestController controller) {
        super(settings, licenseState);
        controller.registerHandler(RestRequest.Method.PUT, "/_security/api_key", this);
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        return null;
    }

    @Override
    public String getName() {
        return "create_api_key";
    }
}
