/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.rest.action.authz.cache;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.shield.action.authz.cache.ClearRolesCacheRequest;
import org.elasticsearch.shield.action.authz.cache.ClearRolesCacheResponse;
import org.elasticsearch.shield.client.ShieldClient;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 *
 */
public class RestClearRolesCacheAction extends BaseRestHandler {

    @Inject
    public RestClearRolesCacheAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_shield/roles/{roles}/_cache/clear", this);
    }

    @Override
    protected void handleRequest(RestRequest request, final RestChannel channel, Client client) throws Exception {

        String[] roles = request.paramAsStringArrayOrEmptyIfAll("roles");

        ClearRolesCacheRequest req = new ClearRolesCacheRequest().roles(roles);

        new ShieldClient(client).clearRolesCache(req, new RestBuilderListener<ClearRolesCacheResponse>(channel) {
            @Override
            public RestResponse buildResponse(ClearRolesCacheResponse response, XContentBuilder builder) throws Exception {
                response.toXContent(builder, ToXContent.EMPTY_PARAMS);
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
