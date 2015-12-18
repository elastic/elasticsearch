/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.rest.action;

import org.elasticsearch.ElasticsearchSecurityException;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationService;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestAuthenticateAction extends BaseRestHandler {

    private final AuthenticationService authenticationService;
    @Inject
    public RestAuthenticateAction(Settings settings, RestController controller, Client client, AuthenticationService authenticationService) {
        super(settings, controller, client);
        this.authenticationService = authenticationService;
        controller.registerHandler(GET, "/_shield/authenticate", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        // we should be authenticated at this point, but we call the authc service to retrieve the user from the context
        User user = authenticationService.authenticate(request);
        assert user != null;
        if (user.isSystem()) {
            throw new ElasticsearchSecurityException("the authenticate API cannot be used for the internal system user");
        }
        XContentBuilder builder = channel.newBuilder();
        user.toXContent(builder, ToXContent.EMPTY_PARAMS);
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
    }
}
