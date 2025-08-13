/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.application.EnterpriseSearch;
import org.elasticsearch.xpack.application.EnterpriseSearchBaseRestHandler;
import org.elasticsearch.xpack.application.utils.LicenseUtils;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestGetSearchApplicationAction extends EnterpriseSearchBaseRestHandler {
    public RestGetSearchApplicationAction(XPackLicenseState licenseState) {
        super(licenseState, LicenseUtils.Product.SEARCH_APPLICATION);
    }

    @Override
    public String getName() {
        return "search_application_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/" + EnterpriseSearch.SEARCH_APPLICATION_API_ENDPOINT + "/{name}"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest restRequest, NodeClient client) {
        GetSearchApplicationAction.Request request = new GetSearchApplicationAction.Request(restRequest.param("name"));
        return channel -> client.execute(GetSearchApplicationAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
