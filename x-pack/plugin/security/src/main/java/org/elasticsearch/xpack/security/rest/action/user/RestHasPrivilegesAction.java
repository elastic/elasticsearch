/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST handler that tests whether a user has the specified
 * {@link RoleDescriptor.IndicesPrivileges privileges}
 */
public class RestHasPrivilegesAction extends SecurityBaseRestHandler {

    private final SecurityContext securityContext;

    public RestHasPrivilegesAction(Settings settings, RestController controller, SecurityContext securityContext,
                                   XPackLicenseState licenseState) {
        super(settings, licenseState);
        this.securityContext = securityContext;
        controller.registerHandler(GET, "/_xpack/security/user/{username}/_has_privileges", this);
        controller.registerHandler(POST, "/_xpack/security/user/{username}/_has_privileges", this);
        controller.registerHandler(GET, "/_xpack/security/user/_has_privileges", this);
        controller.registerHandler(POST, "/_xpack/security/user/_has_privileges", this);
    }

    @Override
    public String getName() {
        return "xpack_security_has_priviledges_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String username = getUsername(request);
        final Tuple<XContentType, BytesReference> content = request.contentOrSourceParam();
        HasPrivilegesRequestBuilder requestBuilder = new SecurityClient(client).prepareHasPrivileges(username, content.v2(), content.v1());
        return channel -> requestBuilder.execute(new HasPrivilegesRestResponseBuilder(username, channel));
    }

    private String getUsername(RestRequest request) {
        final String username = request.param("username");
        if (username != null) {
            return username;
        }
        return securityContext.getUser().principal();
    }

    static class HasPrivilegesRestResponseBuilder extends RestBuilderListener<HasPrivilegesResponse> {
        private String username;

        HasPrivilegesRestResponseBuilder(String username, RestChannel channel) {
            super(channel);
            this.username = username;
        }

        @Override
        public RestResponse buildResponse(HasPrivilegesResponse response, XContentBuilder builder) throws Exception {
            builder.startObject()
                    .field("username", username)
                    .field("has_all_requested", response.isCompleteMatch());

            builder.field("cluster");
            builder.map(response.getClusterPrivileges());

            appendResources(builder, "index", response.getIndexPrivileges());

            builder.startObject("application");
            final Map<String, List<HasPrivilegesResponse.ResourcePrivileges>> appPrivileges = response.getApplicationPrivileges();
            for (String app : appPrivileges.keySet()) {
                appendResources(builder, app, appPrivileges.get(app));
            }
            builder.endObject();

            builder.endObject();
            return new BytesRestResponse(RestStatus.OK, builder);
        }

        private void appendResources(XContentBuilder builder, String field, List<HasPrivilegesResponse.ResourcePrivileges> privileges)
                throws IOException {
            builder.startObject(field);
            for (HasPrivilegesResponse.ResourcePrivileges privilege : privileges) {
                builder.field(privilege.getResource());
                builder.map(privilege.getPrivileges());
            }
            builder.endObject();
        }

    }
}
