/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilderFactory;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST handler that tests whether a user has the specified
 * {@link RoleDescriptor.IndicesPrivileges privileges}
 */
@ServerlessScope(Scope.PUBLIC)
public class RestHasPrivilegesAction extends SecurityBaseRestHandler {

    private final SecurityContext securityContext;
    private final HasPrivilegesRequestBuilderFactory builderFactory;

    public RestHasPrivilegesAction(
        Settings settings,
        SecurityContext securityContext,
        XPackLicenseState licenseState,
        HasPrivilegesRequestBuilderFactory builderFactory
    ) {
        super(settings, licenseState);
        this.securityContext = securityContext;
        this.builderFactory = builderFactory;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_security/user/{username}/_has_privileges"),
            new Route(POST, "/_security/user/{username}/_has_privileges"),
            new Route(GET, "/_security/user/_has_privileges"),
            new Route(POST, "/_security/user/_has_privileges")
        );
    }

    @Override
    public String getName() {
        return "security_has_privileges_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        /*
         * Consume the body immediately. This ensures that if there is a body and we later reject the request (e.g., because security is not
         * enabled) that the REST infrastructure will not reject the request for not having consumed the body.
         */
        final Tuple<XContentType, BytesReference> content = request.contentOrSourceParam();
        final String username = getUsername(request);
        if (username == null) {
            return restChannel -> { throw new ElasticsearchSecurityException("there is no authenticated user"); };
        }
        final HasPrivilegesRequestBuilder requestBuilder = builderFactory.create(client).source(username, content.v2(), content.v1());
        return channel -> requestBuilder.execute(new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(HasPrivilegesResponse response, XContentBuilder builder) throws Exception {
                response.toXContent(builder, ToXContent.EMPTY_PARAMS);
                return new RestResponse(RestStatus.OK, builder);
            }
        });
    }

    private String getUsername(RestRequest request) {
        final String username = request.param("username");
        if (username != null) {
            return username;
        }
        final User user = securityContext.getUser();
        if (user == null) {
            return null;
        }
        return user.principal();
    }
}
