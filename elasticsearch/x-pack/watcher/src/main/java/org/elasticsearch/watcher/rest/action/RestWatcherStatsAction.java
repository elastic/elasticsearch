/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.rest.WatcherRestHandler;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsRequest;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsResponse;

import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestWatcherStatsAction extends WatcherRestHandler {

    @Inject
    public RestWatcherStatsAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(GET, URI_BASE + "/stats", this);
        controller.registerHandler(GET, URI_BASE + "/stats/{metric}", this);
    }

    @Override
    protected void handleRequest(final RestRequest restRequest, RestChannel restChannel, WatcherClient client) throws Exception {
        Set<String> metrics = Strings.splitStringByCommaToSet(restRequest.param("metric", ""));

        WatcherStatsRequest request = new WatcherStatsRequest();
        if (metrics.contains("_all")) {
            request.includeCurrentWatches(true);
            request.includeQueuedWatches(true);
        } else {
            request.includeCurrentWatches(metrics.contains("queued_watches"));
            request.includeQueuedWatches(metrics.contains("pending_watches"));
        }

        client.watcherStats(request, new RestBuilderListener<WatcherStatsResponse>(restChannel) {
            @Override
            public RestResponse buildResponse(WatcherStatsResponse watcherStatsResponse, XContentBuilder builder) throws Exception {
                watcherStatsResponse.toXContent(builder, restRequest);
                return new BytesRestResponse(OK, builder);
            }
        });
    }
}
