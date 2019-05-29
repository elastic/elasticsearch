/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.realm;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public final class RestClearRealmCacheAction extends SecurityBaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestClearRealmCacheAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public RestClearRealmCacheAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        controller.registerHandler(POST, "/_xpack/security/realm/{realms}/_clear_cache", this);
        controller.registerHandler(POST, "/_security/realm/{realms}/_clear_cache", this);

        // @deprecated: Remove in 6.0
        controller.registerAsDeprecatedHandler(POST, "/_shield/realm/{realms}/_cache/clear", this,
                                               "[POST /_shield/realm/{realms}/_cache/clear] is deprecated! Use " +
                                               "[POST /_xpack/security/realm/{realms}/_clear_cache] instead.",
                                               deprecationLogger);
        controller.registerAsDeprecatedHandler(POST, "/_shield/realm/{realms}/_clear_cache", this,
                                               "[POST /_shield/realm/{realms}/_clear_cache] is deprecated! Use " +
                                               "[POST /_xpack/security/realm/{realms}/_clear_cache] instead.",
                                               deprecationLogger);
    }

    @Override
    public String getName() {
        return "xpack_security_clear_realm_cache_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] realms = request.paramAsStringArrayOrEmptyIfAll("realms");
        String[] usernames = request.paramAsStringArrayOrEmptyIfAll("usernames");

        ClearRealmCacheRequest req = new ClearRealmCacheRequest().realms(realms).usernames(usernames);

        return channel -> new SecurityClient(client).clearRealmCache(req, new NodesResponseRestListener<>(channel));
    }

}
