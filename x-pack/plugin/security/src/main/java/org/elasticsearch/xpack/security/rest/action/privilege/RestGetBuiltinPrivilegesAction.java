/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.privilege;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesResponse;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest action to retrieve an application privilege from the security index
 */
public class RestGetBuiltinPrivilegesAction extends SecurityBaseRestHandler {

    public RestGetBuiltinPrivilegesAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/privilege/_builtin"));
    }

    @Override
    public String getName() {
        return "security_get_builtin_privileges_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> client.execute(GetBuiltinPrivilegesAction.INSTANCE, new GetBuiltinPrivilegesRequest(),
            new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(GetBuiltinPrivilegesResponse response, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    builder.array("cluster", response.getClusterPrivileges());
                    builder.array("index", response.getIndexPrivileges());
                    builder.endObject();
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
    }

}
