/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.rolemapping;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingResponse;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Rest endpoint to delete a role-mapping from the {@link org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore}
 */
public class RestDeleteRoleMappingAction extends SecurityBaseRestHandler {

    public RestDeleteRoleMappingAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // TODO: remove deprecated endpoint in 8.0.0
        return Collections.singletonList(
            new ReplacedRoute(DELETE, "/_security/role_mapping/{name}", DELETE, "/_xpack/security/role_mapping/{name}")
        );
    }

    @Override
    public String getName() {
        return "security_delete_role_mapping_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String name = request.param("name");
        final String refresh = request.param("refresh");

        return channel -> new DeleteRoleMappingRequestBuilder(client)
            .name(name)
            .setRefreshPolicy(refresh)
            .execute(new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(DeleteRoleMappingResponse response, XContentBuilder builder) throws Exception {
                    return new BytesRestResponse(response.isFound() ? RestStatus.OK : RestStatus.NOT_FOUND,
                            builder.startObject().field("found", response.isFound()).endObject());
                }
            });
    }
}
