/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.searchablesnapshots.action.RepositoryStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.RepositoryStatsRequest;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestRepositoryStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "repository_stats_action";
    }

    @Override
    public List<RestHandler.Route> routes() {
        return Collections.singletonList(new RestHandler.Route(GET, "/_snapshot/{repository}/_stats"));
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final RepositoryStatsRequest repositoryStatsRequest = new RepositoryStatsRequest(request.param("repository"));
        return channel -> client.execute(RepositoryStatsAction.INSTANCE, repositoryStatsRequest, new RestToXContentListener<>(channel));
    }
}
