/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.application.EnterpriseSearch;
import org.elasticsearch.xpack.application.EnterpriseSearchBaseRestHandler;
import org.elasticsearch.xpack.application.utils.LicenseUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutQueryRuleAction extends EnterpriseSearchBaseRestHandler {
    public RestPutQueryRuleAction(XPackLicenseState licenseState) {
        super(licenseState, LicenseUtils.Product.QUERY_RULES);
    }

    @Override
    public String getName() {
        return "query_rule_put_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/" + EnterpriseSearch.QUERY_RULES_API_ENDPOINT + "/{ruleset_id}/_rule/{rule_id}"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PutQueryRuleAction.Request request = new PutQueryRuleAction.Request(
            restRequest.param("ruleset_id"),
            restRequest.param("rule_id"),
            restRequest.requiredContent(),
            restRequest.getXContentType()
        );
        return channel -> client.execute(
            PutQueryRuleAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel, PutQueryRuleAction.Response::status, r -> null)
        );
    }
}
