/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.job;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.GetJobsStatsAction;
import org.elasticsearch.xpack.prelert.job.Job;

import java.io.IOException;

public class RestGetJobsStatsAction extends BaseRestHandler {

    private final GetJobsStatsAction.TransportAction transportGetJobsStatsAction;

    @Inject
    public RestGetJobsStatsAction(Settings settings, RestController controller,
                                  GetJobsStatsAction.TransportAction transportGetJobsStatsAction) {
        super(settings);
        this.transportGetJobsStatsAction = transportGetJobsStatsAction;

        controller.registerHandler(RestRequest.Method.GET, PrelertPlugin.BASE_PATH
                + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_stats", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        GetJobsStatsAction.Request request = new GetJobsStatsAction.Request(restRequest.param(Job.ID.getPreferredName()));
        return channel -> transportGetJobsStatsAction.execute(request, new RestStatusToXContentListener<>(channel));
    }
}
