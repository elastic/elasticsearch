/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.security.action.user.SetEnabledRequestBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * REST handler for enabling and disabling users. The username is required and we use the path to determine if the user is being
 * enabled or disabled.
 */
@ServerlessScope(Scope.INTERNAL)
public class RestSetEnabledAction extends NativeUserBaseRestHandler {

    public RestSetEnabledAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_security/user/{username}/_enable"),
            new Route(PUT, "/_security/user/{username}/_enable"),
            new Route(POST, "/_security/user/{username}/_disable"),
            new Route(PUT, "/_security/user/{username}/_disable")
        );
    }

    @Override
    public String getName() {
        return "security_set_enabled_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        // TODO consider splitting up enable and disable to have their own rest handler
        final boolean enabled = request.path().endsWith("_enable");
        assert enabled || request.path().endsWith("_disable");
        final String username = request.param("username");
        return channel -> new SetEnabledRequestBuilder(client).username(username)
            .enabled(enabled)
            .execute(new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(ActionResponse.Empty setEnabledResponse, XContentBuilder builder) throws Exception {
                    return new RestResponse(RestStatus.OK, builder.startObject().endObject());
                }
            });
    }
}
