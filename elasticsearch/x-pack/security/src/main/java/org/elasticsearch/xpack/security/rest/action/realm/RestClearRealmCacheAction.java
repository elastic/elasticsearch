/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.realm;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions.NodesResponseRestListener;
import org.elasticsearch.xpack.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.security.client.SecurityClient;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestClearRealmCacheAction extends BaseRestHandler {

    @Inject
    public RestClearRealmCacheAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(POST, "/_xpack/security/realm/{realms}/_cache/clear", this); // deprecated
        controller.registerHandler(POST, "/_xpack/security/realm/{realms}/_clear_cache", this);
    }

    @Override
    protected void handleRequest(RestRequest request, final RestChannel channel, Client client) throws Exception {

        String[] realms = request.paramAsStringArrayOrEmptyIfAll("realms");
        String[] usernames = request.paramAsStringArrayOrEmptyIfAll("usernames");

        ClearRealmCacheRequest req = new ClearRealmCacheRequest().realms(realms).usernames(usernames);

        new SecurityClient(client).clearRealmCache(req, new NodesResponseRestListener<>(channel));
    }

}
