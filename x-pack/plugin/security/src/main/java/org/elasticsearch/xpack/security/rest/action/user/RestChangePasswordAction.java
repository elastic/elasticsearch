/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestChangePasswordAction extends SecurityBaseRestHandler implements RestRequestFilter {

    private final SecurityContext securityContext;
    private final Hasher passwordHasher;

    public RestChangePasswordAction(Settings settings, SecurityContext securityContext, XPackLicenseState licenseState) {
        super(settings, licenseState);
        this.securityContext = securityContext;
        passwordHasher = Hasher.resolve(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings));
    }

    @Override
    public List<Route> routes() {
        return org.elasticsearch.core.List.of(
            Route.builder(PUT, "/_security/user/{username}/_password")
                .replaces(PUT, "/_xpack/security/user/{username}/_password", RestApiVersion.V_7).build(),
            Route.builder(POST, "/_security/user/{username}/_password")
                .replaces(POST, "/_xpack/security/user/{username}/_password", RestApiVersion.V_7).build(),
            Route.builder(PUT, "/_security/user/_password")
                .replaces(PUT, "/_xpack/security/user/_password", RestApiVersion.V_7).build(),
            Route.builder(POST, "/_security/user/_password")
                .replaces(POST, "/_xpack/security/user/_password", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "security_change_password_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final User user = securityContext.getUser();
        final String username;
        if (request.param("username") == null) {
            username = user.principal();
        } else {
            username = request.param("username");
        }

        final String refresh = request.param("refresh");
        final BytesReference content = request.requiredContent();
        return channel ->
                new SecurityClient(client)
                    .prepareChangePassword(username, content, request.getXContentType(), passwordHasher)
                        .setRefreshPolicy(refresh)
                        .execute(new RestBuilderListener<ActionResponse.Empty>(channel) {
                            @Override
                            public RestResponse buildResponse(ActionResponse.Empty changePasswordResponse,
                                                              XContentBuilder builder) throws Exception {
                                return new BytesRestResponse(RestStatus.OK, builder.startObject().endObject());
                            }
                        });
    }

    private static final Set<String> FILTERED_FIELDS = Set.of("password", "password_hash");

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}
