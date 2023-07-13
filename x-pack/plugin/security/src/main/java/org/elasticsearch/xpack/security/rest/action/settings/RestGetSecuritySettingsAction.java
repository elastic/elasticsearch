/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.settings;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.settings.GetSecuritySettingsAction;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

public class RestGetSecuritySettingsAction extends SecurityBaseRestHandler {
    /**
     * @param settings     the node's settings
     * @param licenseState the license state that will be used to determine if security is licensed
     */
    public RestGetSecuritySettingsAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public String getName() {
        return "security_get_settings";
    }

    @Override
    public List<Route> routes() {
        return List.of(Route.builder(RestRequest.Method.GET, "/_security/settings").build());
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        GetSecuritySettingsAction.Request req = new GetSecuritySettingsAction.Request();
        return restChannel -> client.execute(GetSecuritySettingsAction.INSTANCE, req, new RestToXContentListener<>(restChannel));
    }
}
