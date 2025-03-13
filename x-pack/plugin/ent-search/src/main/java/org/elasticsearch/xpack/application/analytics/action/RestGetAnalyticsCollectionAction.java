/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.application.EnterpriseSearch;
import org.elasticsearch.xpack.application.EnterpriseSearchBaseRestHandler;
import org.elasticsearch.xpack.application.utils.LicenseUtils;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * @deprecated in 9.0
 */
@Deprecated
@UpdateForV10(owner = UpdateForV10.Owner.ENTERPRISE_SEARCH)
@ServerlessScope(Scope.PUBLIC)
public class RestGetAnalyticsCollectionAction extends EnterpriseSearchBaseRestHandler {
    public RestGetAnalyticsCollectionAction(XPackLicenseState licenseState) {
        super(licenseState, LicenseUtils.Product.BEHAVIORAL_ANALYTICS);
    }

    @Override
    public String getName() {
        return "get_analytics_collection_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/" + EnterpriseSearch.BEHAVIORAL_ANALYTICS_API_ENDPOINT + "/{collection_name}"),
            new Route(GET, "/" + EnterpriseSearch.BEHAVIORAL_ANALYTICS_API_ENDPOINT)
        );
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest restRequest, NodeClient client) {
        GetAnalyticsCollectionAction.Request request = new GetAnalyticsCollectionAction.Request(
            RestUtils.getMasterNodeTimeout(restRequest),
            Strings.splitStringByCommaToArray(restRequest.param("collection_name"))
        );
        return channel -> client.execute(GetAnalyticsCollectionAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
