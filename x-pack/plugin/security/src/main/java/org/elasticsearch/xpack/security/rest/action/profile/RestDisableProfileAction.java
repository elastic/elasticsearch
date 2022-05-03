/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.profile;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.profile.SetProfileEnabledAction;
import org.elasticsearch.xpack.core.security.action.profile.SetProfileEnabledRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestDisableProfileAction extends SecurityBaseRestHandler {

    public RestDisableProfileAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, "/_security/profile/{uid}/_disable").build(),
            Route.builder(PUT, "/_security/profile/{uid}/_disable").build()
        );
    }

    @Override
    public String getName() {
        return "security_disable_profile_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String uid = request.param("uid");
        final WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.parse(request.param("refresh", "wait_for"));
        return channel -> client.execute(
            SetProfileEnabledAction.INSTANCE,
            new SetProfileEnabledRequest(uid, false, refreshPolicy),
            new RestToXContentListener<>(channel)
        );
    }
}
