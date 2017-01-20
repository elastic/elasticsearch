/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.security.SecurityContext;
import org.elasticsearch.xpack.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.security.user.User;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestAuthenticateAction extends BaseRestHandler {

    private final SecurityContext securityContext;
    private final XPackLicenseState licenseState;

    public RestAuthenticateAction(Settings settings, RestController controller, SecurityContext securityContext,
                                  XPackLicenseState licenseState) {
        super(settings);
        this.securityContext = securityContext;
        this.licenseState = licenseState;
        controller.registerHandler(GET, "/_xpack/security/_authenticate", this);

        // @deprecated: Remove in 6.0
        controller.registerAsDeprecatedHandler(GET, "/_shield/authenticate", this,
                                               "[GET /_shield/authenticate] is deprecated! Use " +
                                               "[GET /_xpack/security/_authenticate] instead.",
                                               deprecationLogger);
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // this API is a special case since we access the user here and we want it to fail with the proper error instead of a request
        // validation error
        if (licenseState.isAuthAllowed() == false) {
            throw LicenseUtils.newComplianceException(XPackPlugin.SECURITY);
        }

        final User user = securityContext.getUser();
        assert user != null;
        final String username = user.runAs() == null ? user.principal() : user.runAs().principal();

        return channel -> client.execute(AuthenticateAction.INSTANCE, new AuthenticateRequest(username),
                new RestBuilderListener<AuthenticateResponse>(channel) {
            @Override
            public RestResponse buildResponse(AuthenticateResponse authenticateResponse, XContentBuilder builder) throws Exception {
                authenticateResponse.user().toXContent(builder, ToXContent.EMPTY_PARAMS);
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });

    }
}
