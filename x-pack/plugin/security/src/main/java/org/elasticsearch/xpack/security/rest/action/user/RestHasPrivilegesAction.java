/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST handler that tests whether a user has the specified
 * {@link RoleDescriptor.IndicesPrivileges privileges}
 */
public class RestHasPrivilegesAction extends SecurityBaseRestHandler {

    private final SecurityContext securityContext;

    public RestHasPrivilegesAction(Settings settings, SecurityContext securityContext, XPackLicenseState licenseState) {
        super(settings, licenseState);
        this.securityContext = securityContext;
    }

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // TODO: remove deprecated endpoint in 8.0.0
        return List.of(
            new ReplacedRoute(GET, "/_security/user/{username}/_has_privileges",
                GET, "/_xpack/security/user/{username}/_has_privileges"),
            new ReplacedRoute(POST, "/_security/user/{username}/_has_privileges",
                POST, "/_xpack/security/user/{username}/_has_privileges"),
            new ReplacedRoute(GET, "/_security/user/_has_privileges",
                GET, "/_xpack/security/user/_has_privileges"),
            new ReplacedRoute(POST, "/_security/user/_has_privileges",
                POST, "/_xpack/security/user/_has_privileges")
        );
    }

    @Override
    public String getName() {
        return "security_has_priviledges_action";
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
        HasPrivilegesRequestBuilder requestBuilder = new HasPrivilegesRequestBuilder(client).source(username, content.v2(), content.v1());
        return channel -> requestBuilder.execute(new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(HasPrivilegesResponse response, XContentBuilder builder) throws Exception {
                response.toXContent(builder, ToXContent.EMPTY_PARAMS);
                return new BytesRestResponse(RestStatus.OK, builder);
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
