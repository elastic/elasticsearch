/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.searchablesnapshots.action.RepositoryStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.RepositoryStatsRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * @deprecated This API is superseded by the Repositories Metering API
 */
@Deprecated
public class RestRepositoryStatsAction extends BaseRestHandler {

    private static final String ENDPOINT = "/_snapshot/{repository}/_stats";

    @Override
    public String getName() {
        return "repository_stats_action";
    }

    @Override
    public List<Route> routes() {
        return org.elasticsearch.core.List.of(
            Route.builder(GET, ENDPOINT)
                .deprecated(
                    "["
                        + ENDPOINT
                        + "] is deprecated, use the Repositories Metering API "
                        + "[/_nodes/{nodeId}/_repositories_metering] in the future.",
                    RestApiVersion.V_7
                )
                .build()
        );
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final RepositoryStatsRequest repositoryStatsRequest = new RepositoryStatsRequest(request.param("repository"));
        return channel -> client.execute(RepositoryStatsAction.INSTANCE, repositoryStatsRequest, new RestToXContentListener<>(channel));
    }
}
