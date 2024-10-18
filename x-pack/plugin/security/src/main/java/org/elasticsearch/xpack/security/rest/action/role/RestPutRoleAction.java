/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.role;

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
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilder;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilderFactory;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Rest endpoint to add a Role to the security index
 */
@ServerlessScope(Scope.PUBLIC)
public class RestPutRoleAction extends NativeRoleBaseRestHandler {

    private final PutRoleRequestBuilderFactory builderFactory;

    public RestPutRoleAction(Settings settings, XPackLicenseState licenseState, PutRoleRequestBuilderFactory builderFactory) {
        super(settings, licenseState);
        this.builderFactory = builderFactory;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/role/{name}"), new Route(PUT, "/_security/role/{name}"));
    }

    @Override
    public String getName() {
        return "security_put_role_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final PutRoleRequestBuilder requestBuilder = builderFactory.create(client)
            .source(request.param("name"), request.requiredContent(), request.getXContentType())
            .setRefreshPolicy(request.param("refresh"));
        return channel -> requestBuilder.execute(new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(PutRoleResponse putRoleResponse, XContentBuilder builder) throws Exception {
                return new RestResponse(RestStatus.OK, builder.startObject().field("role", putRoleResponse).endObject());
            }
        });
    }
}
