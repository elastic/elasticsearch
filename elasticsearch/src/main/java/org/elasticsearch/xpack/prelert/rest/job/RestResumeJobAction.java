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
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.ResumeJobAction;
import org.elasticsearch.xpack.prelert.job.Job;

import java.io.IOException;

public class RestResumeJobAction extends BaseRestHandler {

    private final ResumeJobAction.TransportAction transportResumeJobAction;

    @Inject
    public RestResumeJobAction(Settings settings, RestController controller, ResumeJobAction.TransportAction transportResumeJobAction) {
        super(settings);
        this.transportResumeJobAction = transportResumeJobAction;
        controller.registerHandler(RestRequest.Method.POST, PrelertPlugin.BASE_PATH + "jobs/{" + Job.ID.getPreferredName() + "}/_resume",
                this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        ResumeJobAction.Request request = new ResumeJobAction.Request(restRequest.param(Job.ID.getPreferredName()));
        return channel -> transportResumeJobAction.execute(request, new AcknowledgedRestListener<>(channel));
    }
}
