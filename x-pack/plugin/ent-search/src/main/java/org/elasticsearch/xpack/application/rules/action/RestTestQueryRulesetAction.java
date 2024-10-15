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

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestTestQueryRulesetAction extends EnterpriseSearchBaseRestHandler {
    public RestTestQueryRulesetAction(XPackLicenseState licenseState) {
        super(licenseState, LicenseUtils.Product.QUERY_RULES);
    }

    @Override
    public String getName() {
        return "query_ruleset_test_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/" + EnterpriseSearch.QUERY_RULES_API_ENDPOINT + "/{ruleset_id}" + "/_test"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final String rulesetId = restRequest.param("ruleset_id");
        TestQueryRulesetAction.Request request = null;
        if (restRequest.hasContent()) {
            try (var parser = restRequest.contentParser()) {
                request = TestQueryRulesetAction.Request.parse(parser, rulesetId);
            }
        }
        final TestQueryRulesetAction.Request finalRequest = request;
        return channel -> client.execute(TestQueryRulesetAction.INSTANCE, finalRequest, new RestToXContentListener<>(channel));
    }
}
