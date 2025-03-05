/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.rolemapping;

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
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Rest endpoint to delete a role-mapping from the {@link org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore}
 */
@ServerlessScope(Scope.INTERNAL)
public class RestDeleteRoleMappingAction extends NativeRoleMappingBaseRestHandler {

    public RestDeleteRoleMappingAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_security/role_mapping/{name}"));
    }

    @Override
    public String getName() {
        return "security_delete_role_mapping_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String name = request.param("name");
        final String refresh = request.param("refresh");

        return channel -> new DeleteRoleMappingRequestBuilder(client).name(name)
            .setRefreshPolicy(refresh)
            .execute(new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(DeleteRoleMappingResponse response, XContentBuilder builder) throws Exception {
                    return new RestResponse(
                        response.isFound() ? RestStatus.OK : RestStatus.NOT_FOUND,
                        builder.startObject().field("found", response.isFound()).endObject()
                    );
                }
            });
    }
}
