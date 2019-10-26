/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.realm;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public final class RestClearRealmCacheAction extends SecurityBaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestClearRealmCacheAction.class));

    public RestClearRealmCacheAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
            POST, "/_security/realm/{realms}/_clear_cache", this,
            POST, "/_xpack/security/realm/{realms}/_clear_cache", deprecationLogger);
    }

    @Override
    public String getName() {
        return "security_clear_realm_cache_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) {
        String[] realms = request.paramAsStringArrayOrEmptyIfAll("realms");
        String[] usernames = request.paramAsStringArrayOrEmptyIfAll("usernames");

        ClearRealmCacheRequest req = new ClearRealmCacheRequest().realms(realms).usernames(usernames);

        return channel -> client.execute(ClearRealmCacheAction.INSTANCE, req, new NodesResponseRestListener<>(channel));
    }

}
