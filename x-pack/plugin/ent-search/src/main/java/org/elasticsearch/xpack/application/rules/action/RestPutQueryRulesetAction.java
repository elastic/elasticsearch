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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.application.EnterpriseSearch;
import org.elasticsearch.xpack.application.EnterpriseSearchBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutQueryRulesetAction extends EnterpriseSearchBaseRestHandler {
    public RestPutQueryRulesetAction(XPackLicenseState licenseState) {
        super(licenseState);
    }

    @Override
    public String getName() {
        return "query_ruleset_put_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/" + EnterpriseSearch.QUERY_RULES_API_ENDPOINT + "/{ruleset_id}"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PutQueryRulesetAction.Request request = new PutQueryRulesetAction.Request(
            restRequest.param("ruleset_id"),
            restRequest.paramAsBoolean("create", false),
            restRequest.content(),
            restRequest.getXContentType()
        );
        return channel -> client.execute(PutQueryRulesetAction.INSTANCE, request, new RestToXContentListener<>(channel) {
            @Override
            protected RestStatus getStatus(PutQueryRulesetAction.Response response) {
                return response.status();
            }
        });
    }
}
