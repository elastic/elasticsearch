/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.rest;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestClearSearchableSnapshotsCacheAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_searchable_snapshots/cache/clear"),
            new Route(POST, "/{index}/_searchable_snapshots/cache/clear")
        );
    }

    @Override
    public String getName() {
        return "clear_indices_searchable_snapshots_cache_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        final ClearSearchableSnapshotsCacheRequest request = new ClearSearchableSnapshotsCacheRequest();
        request.indices(Strings.splitStringByCommaToArray(restRequest.param("index")));
        request.indicesOptions(IndicesOptions.fromRequest(restRequest, request.indicesOptions()));
        return channel -> client.execute(ClearSearchableSnapshotsCacheAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
