/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.stats;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.stats.GetSecurityStatsAction;
import org.elasticsearch.xpack.core.security.action.stats.GetSecurityStatsNodesRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public class RestSecurityStatsAction extends SecurityBaseRestHandler {

    public RestSecurityStatsAction(final Settings settings, final XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/stats"));
    }

    @Override
    public String getName() {
        return "security_stats_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) {
        final var req = new GetSecurityStatsNodesRequest();
        return channel -> client.execute(GetSecurityStatsAction.INSTANCE, req, new RestToXContentListener<>(channel));
    }

}
