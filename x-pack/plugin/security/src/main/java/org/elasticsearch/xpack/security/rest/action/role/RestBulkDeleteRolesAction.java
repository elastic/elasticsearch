/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.role;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.action.ActionTypes;
import org.elasticsearch.xpack.core.security.action.role.BulkDeleteRolesRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Rest endpoint to bulk delete roles to the security index
 */
public class RestBulkDeleteRolesAction extends NativeRoleBaseRestHandler {
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<BulkDeleteRolesRequest, Void> PARSER = new ConstructingObjectParser<>(
        "bulk_delete_roles_request",
        a -> new BulkDeleteRolesRequest((List<String>) a[0])
    );

    static {
        PARSER.declareStringArray(constructorArg(), new ParseField("names"));
    }

    public RestBulkDeleteRolesAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_security/role"));
    }

    @Override
    public String getName() {
        return "security_bulk_delete_roles_action";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        BulkDeleteRolesRequest bulkDeleteRolesRequest = PARSER.parse(request.contentParser(), null);
        if (request.param("refresh") != null) {
            bulkDeleteRolesRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.parse(request.param("refresh")));
        }
        return channel -> client.execute(ActionTypes.BULK_DELETE_ROLES, bulkDeleteRolesRequest, new RestToXContentListener<>(channel));
    }
}
