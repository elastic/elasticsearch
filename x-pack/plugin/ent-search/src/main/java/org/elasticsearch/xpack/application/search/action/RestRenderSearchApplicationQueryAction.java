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
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.application.EnterpriseSearch;
import org.elasticsearch.xpack.application.EnterpriseSearchBaseRestHandler;
import org.elasticsearch.xpack.application.utils.LicenseUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestRenderSearchApplicationQueryAction extends EnterpriseSearchBaseRestHandler {
    public RestRenderSearchApplicationQueryAction(XPackLicenseState licenseState) {
        super(licenseState, LicenseUtils.Product.SEARCH_APPLICATION);
    }

    public static final String ENDPOINT_PATH = "/" + EnterpriseSearch.SEARCH_APPLICATION_API_ENDPOINT + "/{name}" + "/_render_query";

    @Override
    public String getName() {
        return "search_application_render_query_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, ENDPOINT_PATH));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final String searchAppName = restRequest.param("name");
        SearchApplicationSearchRequest request;
        if (restRequest.hasContent()) {
            try (var parser = restRequest.contentParser()) {
                request = SearchApplicationSearchRequest.fromXContent(searchAppName, parser);
            }
        } else {
            request = new SearchApplicationSearchRequest(searchAppName);
        }
        final SearchApplicationSearchRequest finalRequest = request;
        return channel -> client.execute(RenderSearchApplicationQueryAction.INSTANCE, finalRequest, new RestToXContentListener<>(channel));
    }
}
