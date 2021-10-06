/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.rolemapping;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsRequestBuilder;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsResponse;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest endpoint to retrieve a role-mapping from the org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore
 */
public class RestGetRoleMappingsAction extends SecurityBaseRestHandler {

    public RestGetRoleMappingsAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(GET, "/_security/role_mapping/")
                .replaces(GET, "/_xpack/security/role_mapping/", RestApiVersion.V_7).build(),
            Route.builder(GET, "/_security/role_mapping/{name}")
                .replaces(GET, "/_xpack/security/role_mapping/{name}", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "security_get_role_mappings_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String[] names = request.paramAsStringArrayOrEmptyIfAll("name");
        return channel -> new GetRoleMappingsRequestBuilder(client)
            .names(names)
            .execute(new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(GetRoleMappingsResponse response, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    for (ExpressionRoleMapping mapping : response.mappings()) {
                        builder.field(mapping.getName(), mapping);
                    }
                    builder.endObject();

                    // if the request specified mapping names, but nothing was found then return a 404 result
                    if (names.length != 0 && response.mappings().length == 0) {
                        return new BytesRestResponse(RestStatus.NOT_FOUND, builder);
                    } else {
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                }
            });
    }
}
