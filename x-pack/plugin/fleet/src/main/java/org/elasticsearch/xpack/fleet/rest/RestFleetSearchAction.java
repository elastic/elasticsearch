/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.rest;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.IntConsumer;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestFleetSearchAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "fleet_search_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_fleet/_search"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SearchRequest searchRequest;
        if (request.hasParam("min_compatible_shard_node")) {
            searchRequest = new SearchRequest(Version.fromString(request.param("min_compatible_shard_node")));
        } else {
            searchRequest = new SearchRequest();
        }

        IntConsumer setSize = size -> searchRequest.source().size(size);
        request.withContentOrSourceParamParserOrNull(
            parser -> RestSearchAction.parseSearchRequest(searchRequest, request, parser, client.getNamedWriteableRegistry(), setSize)
        );

        String[] stringAfterCheckpointsRefreshed = request.paramAsStringArray("after_checkpoints_refreshed", Strings.EMPTY_ARRAY);
        final long[] afterCheckpointsRefreshed = new long[stringAfterCheckpointsRefreshed.length];
        for (int i = 0; i < stringAfterCheckpointsRefreshed.length; ++i) {
            afterCheckpointsRefreshed[i] = Long.parseLong(stringAfterCheckpointsRefreshed[i]);
        }

        searchRequest.setAfterCheckpointsRefreshed(afterCheckpointsRefreshed);

        return channel -> {
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(SearchAction.INSTANCE, searchRequest, new RestStatusToXContentListener<>(channel));
        };
    }

    @Override
    protected Set<String> responseParams() {
        return RestSearchAction.RESPONSE_PARAMS;
    }
}
