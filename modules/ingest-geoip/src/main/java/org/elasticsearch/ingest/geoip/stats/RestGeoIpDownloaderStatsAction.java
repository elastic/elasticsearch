/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGeoIpDownloaderStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "geoip_downloader_stats";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_ingest/geoip/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> client.execute(
            GeoIpDownloaderStatsAction.INSTANCE,
            new GeoIpDownloaderStatsAction.Request(),
            new RestToXContentListener<>(channel)
        );
    }
}
