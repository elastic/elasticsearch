/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.role;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheRequest;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public final class RestClearRolesCacheAction extends SecurityBaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestClearRolesCacheAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public RestClearRolesCacheAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        controller.registerHandler(POST, "/_xpack/security/role/{name}/_clear_cache", this);
        controller.registerHandler(POST, "/_security/role/{name}/_clear_cache", this);

        // @deprecated: Remove in 6.0
        controller.registerAsDeprecatedHandler(POST, "/_shield/role/{name}/_clear_cache", this,
                                               "[POST /_shield/role/{name}/_clear_cache] is deprecated! Use " +
                                               "[POST /_xpack/security/role/{name}/_clear_cache] instead.",
                                               deprecationLogger);
    }

    @Override
    public String getName() {
        return "xpack_security_clear_roles_cache_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] roles = request.paramAsStringArrayOrEmptyIfAll("name");

        ClearRolesCacheRequest req = new ClearRolesCacheRequest().names(roles);

        return channel -> new SecurityClient(client).clearRolesCache(req, new NodesResponseRestListener<>(channel));
    }
}
