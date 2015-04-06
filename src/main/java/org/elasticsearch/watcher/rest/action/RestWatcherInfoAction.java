/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.rest.WatcherRestHandler;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsRequest;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestWatcherInfoAction extends WatcherRestHandler {

    private final WatcherClient watcherClient;

    @Inject
    protected RestWatcherInfoAction(Settings settings, RestController controller, Client client, WatcherClient watcherClient) {
        super(settings, controller, client);
        this.watcherClient = watcherClient;
        controller.registerHandler(GET, URI_BASE, this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel restChannel, WatcherClient client) throws Exception {
        watcherClient.watcherStats(new WatcherStatsRequest(), new RestBuilderListener<WatcherStatsResponse>(restChannel) {
            @Override
            public RestResponse buildResponse(WatcherStatsResponse watcherStatsResponse, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.startObject("version")
                        .field("number", watcherStatsResponse.getVersion().number())
                        .field("build_hash", watcherStatsResponse.getBuild().hash())
                        .field("build_timestamp", watcherStatsResponse.getBuild().timestamp())
                        .field("build_snapshot", watcherStatsResponse.getVersion().snapshot)
                        .endObject().endObject();

                return new BytesRestResponse(OK, builder);

            }
        });
    }
}
