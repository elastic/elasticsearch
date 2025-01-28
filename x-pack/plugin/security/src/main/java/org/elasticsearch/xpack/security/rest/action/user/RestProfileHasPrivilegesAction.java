/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesRequest;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.INTERNAL)
public class RestProfileHasPrivilegesAction extends SecurityBaseRestHandler {

    public RestProfileHasPrivilegesAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/profile/_has_privileges"), new Route(POST, "/_security/profile/_has_privileges"));
    }

    @Override
    public String getName() {
        return "security_profile_has_privileges_action";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        try (XContentParser parser = restRequest.contentOrSourceParamParser()) {
            ProfileHasPrivilegesRequest request = ProfileHasPrivilegesRequest.PARSER.parse(parser, null);
            final HttpChannel httpChannel = restRequest.getHttpChannel();
            return channel -> new RestCancellableNodeClient(client, httpChannel).execute(
                ProfileHasPrivilegesAction.INSTANCE,
                request,
                new RestToXContentListener<>(channel)
            );
        }
    }

    @Override
    protected Exception innerCheckFeatureAvailable(RestRequest request) {
        if (Security.USER_PROFILE_COLLABORATION_FEATURE.check(licenseState)) {
            return null;
        } else {
            return LicenseUtils.newComplianceException(Security.USER_PROFILE_COLLABORATION_FEATURE.getName());
        }
    }
}
