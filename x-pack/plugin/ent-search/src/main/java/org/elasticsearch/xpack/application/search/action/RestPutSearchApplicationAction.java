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

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutSearchApplicationAction extends EnterpriseSearchBaseRestHandler {
    public RestPutSearchApplicationAction(XPackLicenseState licenseState) {
        super(licenseState, LicenseUtils.Product.SEARCH_APPLICATION);
    }

    @Override
    public String getName() {
        return "search_application_put_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/" + EnterpriseSearch.SEARCH_APPLICATION_API_ENDPOINT + "/{name}"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PutSearchApplicationAction.Request request = new PutSearchApplicationAction.Request(
            restRequest.param("name"),
            restRequest.paramAsBoolean("create", false),
            restRequest.content(),
            restRequest.getXContentType()
        );
        return channel -> client.execute(
            PutSearchApplicationAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel, PutSearchApplicationAction.Response::status, r -> null)
        );
    }
}
