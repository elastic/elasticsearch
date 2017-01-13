/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.schedulers;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.action.PutSchedulerAction;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfig;

import java.io.IOException;

public class RestPutSchedulerAction extends BaseRestHandler {

    private final PutSchedulerAction.TransportAction transportPutSchedulerAction;

    @Inject
    public RestPutSchedulerAction(Settings settings, RestController controller,
                                  PutSchedulerAction.TransportAction transportPutSchedulerAction) {
        super(settings);
        this.transportPutSchedulerAction = transportPutSchedulerAction;
        controller.registerHandler(RestRequest.Method.PUT, MlPlugin.BASE_PATH + "schedulers/{"
                + SchedulerConfig.ID.getPreferredName() + "}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String schedulerId = restRequest.param(SchedulerConfig.ID.getPreferredName());
        XContentParser parser = restRequest.contentParser();
        PutSchedulerAction.Request putSchedulerRequest = PutSchedulerAction.Request.parseRequest(schedulerId, parser);
        return channel -> transportPutSchedulerAction.execute(putSchedulerRequest, new RestToXContentListener<>(channel));
    }

}
