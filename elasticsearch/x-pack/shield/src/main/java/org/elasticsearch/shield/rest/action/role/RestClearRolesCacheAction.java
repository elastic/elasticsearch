/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.rest.action.role;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions.NodesResponseRestListener;
import org.elasticsearch.shield.action.role.ClearRolesCacheRequest;
import org.elasticsearch.shield.client.SecurityClient;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 *
 */
public class RestClearRolesCacheAction extends BaseRestHandler {

    @Inject
    public RestClearRolesCacheAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(POST, "/_xpack/security/role/{name}/_clear_cache", this);
    }

    @Override
    protected void handleRequest(RestRequest request, final RestChannel channel, Client client) throws Exception {

        String[] roles = request.paramAsStringArrayOrEmptyIfAll("name");

        ClearRolesCacheRequest req = new ClearRolesCacheRequest().names(roles);

        new SecurityClient(client).clearRolesCache(req, new NodesResponseRestListener<>(channel));
    }
}
