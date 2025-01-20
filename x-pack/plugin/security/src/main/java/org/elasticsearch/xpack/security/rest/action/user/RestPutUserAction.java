/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Rest endpoint to add a User to the security index
 */
@ServerlessScope(Scope.INTERNAL)
public class RestPutUserAction extends NativeUserBaseRestHandler implements RestRequestFilter {

    private final Hasher passwordHasher;

    public RestPutUserAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
        passwordHasher = Hasher.resolve(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings));
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/user/{username}"), new Route(PUT, "/_security/user/{username}"));
    }

    @Override
    public String getName() {
        return "security_put_user_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        PutUserRequestBuilder requestBuilder = new PutUserRequestBuilder(client).source(
            request.param("username"),
            request.requiredContent(),
            request.getXContentType(),
            passwordHasher
        ).setRefreshPolicy(request.param("refresh"));

        return channel -> requestBuilder.execute(new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(PutUserResponse putUserResponse, XContentBuilder builder) throws Exception {
                putUserResponse.toXContent(builder, request);
                return new RestResponse(RestStatus.OK, builder);
            }
        });
    }

    private static final Set<String> FILTERED_FIELDS = Set.of("password", "password_hash");

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}
