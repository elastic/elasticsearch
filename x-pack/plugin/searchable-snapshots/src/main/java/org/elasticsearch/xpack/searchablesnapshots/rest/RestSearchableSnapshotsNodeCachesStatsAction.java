/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotsNodeCachesStatsAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Node level stats for searchable snapshots caches.
 */
public class RestSearchableSnapshotsNodeCachesStatsAction extends BaseRestHandler {

    @Override
    public List<RestHandler.Route> routes() {
        return List.of(
            new RestHandler.Route(GET, "/_searchable_snapshots/cache/stats"),
            new RestHandler.Route(GET, "/_searchable_snapshots/{nodeId}/cache/stats")
        );
    }

    @Override
    public String getName() {
        return "searchable_snapshots_cache_stats_action";
    }

    @Override
    public BaseRestHandler.RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        return channel -> client.execute(
            TransportSearchableSnapshotsNodeCachesStatsAction.TYPE,
            new TransportSearchableSnapshotsNodeCachesStatsAction.NodesRequest(nodesIds),
            new RestToXContentListener<>(channel)
        );
    }
}
