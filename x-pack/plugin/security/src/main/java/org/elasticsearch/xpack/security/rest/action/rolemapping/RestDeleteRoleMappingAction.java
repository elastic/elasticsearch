/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.rolemapping;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingResponse;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Rest endpoint to delete a role-mapping from the {@link org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore}
 */
public class RestDeleteRoleMappingAction extends SecurityBaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestDeleteRoleMappingAction.class));

    public RestDeleteRoleMappingAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
            DELETE, "/_security/role_mapping/{name}", this,
            DELETE, "/_xpack/security/role_mapping/{name}", deprecationLogger);
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
