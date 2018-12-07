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
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsResponse;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest endpoint to retrieve a role-mapping from the org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore
 */
public class RestGetRoleMappingsAction extends SecurityBaseRestHandler {

    public RestGetRoleMappingsAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        controller.registerHandler(GET, "/_xpack/security/role_mapping/", this);
        controller.registerHandler(GET, "/_security/role_mapping/", this);
        controller.registerHandler(GET, "/_xpack/security/role_mapping/{name}", this);
        controller.registerHandler(GET, "/_security/role_mapping/{name}", this);
    }

    @Override
    public String getName() {
        return "xpack_security_get_role_mappings_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String[] names = request.paramAsStringArrayOrEmptyIfAll("name");
        return channel -> new SecurityClient(client).prepareGetRoleMappings(names)
                .execute(new RestBuilderListener<GetRoleMappingsResponse>(channel) {
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
