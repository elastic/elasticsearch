/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.xpack.security.SecurityContext;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.security.action.user.AuthenticateResponse;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestAuthenticateAction extends BaseRestHandler {

    private final SecurityContext securityContext;

    @Inject
    public RestAuthenticateAction(Settings settings, RestController controller, Client client, SecurityContext securityContext) {
        super(settings, client);
        this.securityContext = securityContext;
        controller.registerHandler(GET, "/_xpack/security/_authenticate", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        final User user = securityContext.getUser();
        assert user != null;
        final String username = user.runAs() == null ? user.principal() : user.runAs().principal();

        client.execute(AuthenticateAction.INSTANCE, new AuthenticateRequest(username),
                new RestBuilderListener<AuthenticateResponse>(channel) {
            @Override
            public RestResponse buildResponse(AuthenticateResponse authenticateResponse, XContentBuilder builder) throws Exception {
                authenticateResponse.user().toXContent(builder, ToXContent.EMPTY_PARAMS);
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });

    }
}
