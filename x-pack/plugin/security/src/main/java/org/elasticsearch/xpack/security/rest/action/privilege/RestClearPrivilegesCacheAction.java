/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.rest.action.privilege;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheAction;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestClearPrivilegesCacheAction extends SecurityBaseRestHandler {

    public RestClearPrivilegesCacheAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public String getName() {
        return "security_clear_privileges_cache_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(POST, "/_security/privilege/{application}/_clear_cache"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] applicationNames = request.paramAsStringArrayOrEmptyIfAll("application");
        final ClearPrivilegesCacheRequest req = new ClearPrivilegesCacheRequest().applicationNames(applicationNames);
        return channel -> client.execute(ClearPrivilegesCacheAction.INSTANCE, req, new NodesResponseRestListener<>(channel));
    }

}
