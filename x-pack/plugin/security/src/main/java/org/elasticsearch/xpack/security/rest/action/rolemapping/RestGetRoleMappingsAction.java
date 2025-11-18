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
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsRequestBuilder;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsResponse;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest endpoint to retrieve a role-mapping from the org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore
 */
@ServerlessScope(Scope.INTERNAL)
public class RestGetRoleMappingsAction extends NativeRoleMappingBaseRestHandler {

    public RestGetRoleMappingsAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/role_mapping/"), new Route(GET, "/_security/role_mapping/{name}"));
    }

    @Override
    public String getName() {
        return "security_get_role_mappings_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String[] names = request.paramAsStringArrayOrEmptyIfAll("name");
        return channel -> new GetRoleMappingsRequestBuilder(client).names(names).execute(new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(GetRoleMappingsResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                for (ExpressionRoleMapping mapping : response.mappings()) {
                    builder.field(mapping.getName(), mapping);
                }
                builder.endObject();

                // if the request specified mapping names, but nothing was found then return a 404 result
                if (names.length != 0 && response.mappings().length == 0) {
                    return new RestResponse(RestStatus.NOT_FOUND, builder);
                } else {
                    return new RestResponse(RestStatus.OK, builder);
                }
            }
        });
    }
}
