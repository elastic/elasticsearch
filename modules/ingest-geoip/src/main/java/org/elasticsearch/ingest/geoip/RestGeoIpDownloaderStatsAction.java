/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.TaskInfo;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGeoIpDownloaderStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "geoip_downloader_stats";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "_geoip/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        ListTasksRequest listTasksRequest = new ListTasksRequest()
            .setDetailed(true)
            .setActions(GeoIpDownloader.GEOIP_DOWNLOADER + "[c]");
        return channel -> {
            RestToXContentListener<ToXContentObject> listener = new RestToXContentListener<>(channel);
            client.execute(ListTasksAction.INSTANCE, listTasksRequest, ActionListener.wrap(res -> {
                List<TaskInfo> tasks = res.getTasks();
                if (tasks.isEmpty()) {
                    listener.onResponse(GeoIpDownloaderStats.EMPTY);
                } else {
                    listener.onResponse(tasks.get(0).getStatus());
                }
            }, listener::onFailure));
        };
    }
}
