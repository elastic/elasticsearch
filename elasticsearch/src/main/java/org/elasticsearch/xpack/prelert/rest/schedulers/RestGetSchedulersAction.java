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
import org.elasticsearch.xpack.prelert.action.GetSchedulersAction;
import org.elasticsearch.xpack.prelert.scheduler.SchedulerConfig;

import java.io.IOException;

public class RestGetSchedulersAction extends BaseRestHandler {

    private final GetSchedulersAction.TransportAction transportGetSchedulersAction;

    @Inject
    public RestGetSchedulersAction(Settings settings, RestController controller,
                                   GetSchedulersAction.TransportAction transportGetSchedulersAction) {
        super(settings);
        this.transportGetSchedulersAction = transportGetSchedulersAction;

        controller.registerHandler(RestRequest.Method.GET, PrelertPlugin.BASE_PATH
                + "schedulers/{" + SchedulerConfig.ID.getPreferredName() + "}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        GetSchedulersAction.Request request = new GetSchedulersAction.Request(restRequest.param(SchedulerConfig.ID.getPreferredName()));
        return channel -> transportGetSchedulersAction.execute(request, new RestStatusToXContentListener<>(channel));
    }
}
