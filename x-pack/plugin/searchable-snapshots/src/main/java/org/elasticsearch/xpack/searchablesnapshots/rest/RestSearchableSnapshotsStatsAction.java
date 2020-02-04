/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsRequest;

public class RestSearchableSnapshotsStatsAction extends BaseRestHandler {

    public RestSearchableSnapshotsStatsAction(final RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, "/_searchable_snapshots/stats", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_searchable_snapshots/stats", this);
    }

    @Override
    public String getName() {
        return "searchable_snapshots_stats_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        String[] indices = Strings.splitStringByCommaToArray(restRequest.param("index"));
        return channel -> client.execute(SearchableSnapshotsStatsAction.INSTANCE,
            new SearchableSnapshotsStatsRequest(indices), new RestToXContentListener<>(channel));
    }
}
