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
import org.elasticsearch.xpack.prelert.action.JobDataAction;
import org.elasticsearch.xpack.prelert.job.Job;

import java.io.IOException;

public class RestJobDataAction extends BaseRestHandler {

    private static final boolean DEFAULT_IGNORE_DOWNTIME = false;
    private static final String DEFAULT_RESET_START = "";
    private static final String DEFAULT_RESET_END = "";

    private final JobDataAction.TransportAction transportPostDataAction;

    @Inject
    public RestJobDataAction(Settings settings, RestController controller, JobDataAction.TransportAction transportPostDataAction) {
        super(settings);
        this.transportPostDataAction = transportPostDataAction;
        controller.registerHandler(RestRequest.Method.POST, PrelertPlugin.BASE_PATH
                + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/data", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        JobDataAction.Request request = new JobDataAction.Request(restRequest.param(Job.ID.getPreferredName()));
        request.setIgnoreDowntime(
                restRequest.paramAsBoolean(JobDataAction.Request.IGNORE_DOWNTIME.getPreferredName(), DEFAULT_IGNORE_DOWNTIME));
        request.setResetStart(restRequest.param(JobDataAction.Request.RESET_START.getPreferredName(), DEFAULT_RESET_START));
        request.setResetEnd(restRequest.param(JobDataAction.Request.RESET_END.getPreferredName(), DEFAULT_RESET_END));
        request.setContent(restRequest.content());

        return channel -> transportPostDataAction.execute(request, new RestStatusToXContentListener<>(channel));
    }
}