/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.profile;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.security.action.profile.GetProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.GetProfileRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetProfileAction extends SecurityBaseRestHandler {

    public RestGetProfileAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/profile/{uid}"));
    }

    @Override
    public String getName() {
        return "xpack_security_get_profile";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String uid = request.param("uid");
        final Set<String> dataKeys = Strings.tokenizeByCommaToSet(request.param("data", null));
        final GetProfileRequest getProfileRequest = new GetProfileRequest(uid, dataKeys);
        return channel -> client.execute(GetProfileAction.INSTANCE, getProfileRequest, new RestStatusToXContentListener<>(channel));
    }
}
