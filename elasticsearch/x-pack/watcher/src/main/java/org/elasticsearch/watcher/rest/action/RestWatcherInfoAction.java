/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.rest.action;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
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

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestWatcherInfoAction extends WatcherRestHandler {

    @Inject
    public RestWatcherInfoAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(GET, URI_BASE, this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel restChannel, WatcherClient client) throws Exception {
        client.watcherStats(new WatcherStatsRequest(), new RestBuilderListener<WatcherStatsResponse>(restChannel) {
            @Override
            public RestResponse buildResponse(WatcherStatsResponse watcherStatsResponse, XContentBuilder builder) throws Exception {
                builder.startObject()
                        .startObject("version")
                            .field("number", Version.CURRENT.toString())
                            .field("build_hash", watcherStatsResponse.getBuild().hash())
                            .field("build_timestamp", watcherStatsResponse.getBuild().timestamp())
                            .field("build_snapshot", Build.CURRENT.isSnapshot())
                        .endObject()
                        .field("tagline", "You Know, for Alerts & Automation")
                        .endObject();
                return new BytesRestResponse(OK, builder);

            }
        });
    }
}
