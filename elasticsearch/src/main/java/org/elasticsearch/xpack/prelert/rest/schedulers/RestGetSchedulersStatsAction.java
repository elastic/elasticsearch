/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.schedulers;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.GetSchedulersStatsAction;
import org.elasticsearch.xpack.prelert.scheduler.SchedulerConfig;

import java.io.IOException;

public class RestGetSchedulersStatsAction extends BaseRestHandler {

    private final GetSchedulersStatsAction.TransportAction transportGetSchedulersStatsAction;

    @Inject
    public RestGetSchedulersStatsAction(Settings settings, RestController controller,
                                        GetSchedulersStatsAction.TransportAction transportGetSchedulersStatsAction) {
        super(settings);
        this.transportGetSchedulersStatsAction = transportGetSchedulersStatsAction;

        controller.registerHandler(RestRequest.Method.GET, PrelertPlugin.BASE_PATH
                + "schedulers/{" + SchedulerConfig.ID.getPreferredName() + "}/_stats", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        GetSchedulersStatsAction.Request request = new GetSchedulersStatsAction.Request(
                restRequest.param(SchedulerConfig.ID.getPreferredName()));
        return channel -> transportGetSchedulersStatsAction.execute(request, new RestStatusToXContentListener<>(channel));
    }
}
