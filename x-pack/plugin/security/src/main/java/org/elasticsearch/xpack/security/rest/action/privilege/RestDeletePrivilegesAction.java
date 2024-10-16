/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.privilege;

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
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesResponse;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Rest action to delete one or more privileges from the security index
 */
@ServerlessScope(Scope.INTERNAL)
public class RestDeletePrivilegesAction extends SecurityBaseRestHandler {

    public RestDeletePrivilegesAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_security/privilege/{application}/{privilege}"));
    }

    @Override
    public String getName() {
        return "security_delete_privilege_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String application = request.param("application");
        final String[] privileges = request.paramAsStringArray("privilege", null);
        final String refresh = request.param("refresh");
        return channel -> new DeletePrivilegesRequestBuilder(client).application(application)
            .privileges(privileges)
            .setRefreshPolicy(refresh)
            .execute(new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(DeletePrivilegesResponse response, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    builder.startObject(application);
                    for (String privilege : new HashSet<>(Arrays.asList(privileges))) {
                        builder.field(privilege, Collections.singletonMap("found", response.found().contains(privilege)));
                    }
                    builder.endObject();
                    builder.endObject();
                    return new RestResponse(response.found().isEmpty() ? RestStatus.NOT_FOUND : RestStatus.OK, builder);
                }
            });
    }

}
