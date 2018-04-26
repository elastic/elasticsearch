/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.privilege;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Rest endpoint to add one or more {@link ApplicationPrivilege} objects to the security index
 */
public class RestPutPrivilegeAction extends SecurityBaseRestHandler {

    public RestPutPrivilegeAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        controller.registerHandler(PUT, "/_xpack/security/privilege/{application}/{privilege}", this);
        controller.registerHandler(POST, "/_xpack/security/privilege/{application}/{privilege}", this);
    }

    @Override
    public String getName() {
        return "xpack_security_put_privilege_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String application = request.param("application");
        final String privilege = request.param("privilege");
        PutPrivilegesRequestBuilder requestBuilder = new SecurityClient(client)
                .preparePutPrivilege(application, privilege, request.requiredContent(), request.getXContentType())
                .setRefreshPolicy(request.param("refresh"));

        return RestPutPrivilegesAction.execute(requestBuilder);
    }
}
