/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.application.EnterpriseSearch;
import org.elasticsearch.xpack.application.EnterpriseSearchBaseRestHandler;
import org.elasticsearch.xpack.application.utils.LicenseUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutConnectorAction extends EnterpriseSearchBaseRestHandler {
    protected RestPutConnectorAction(XPackLicenseState licenseState) {
        super(licenseState, LicenseUtils.Product.CONNECTOR);
    }

    @Override
    public String getName() {
        return "connector_put_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/" + EnterpriseSearch.CONNECTORS_API_ENDPOINT + "/{connector_id}"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PutConnectorAction.Request request = new PutConnectorAction.Request(
            restRequest.param("connector_id"),
            restRequest.content(),
            restRequest.getXContentType()
        );
        return channel -> client.execute(
            PutConnectorAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel, PutConnectorAction.Response::status, r -> null)
        );
    }
}
