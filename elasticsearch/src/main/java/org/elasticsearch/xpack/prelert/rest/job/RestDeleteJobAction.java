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
import org.elasticsearch.xpack.prelert.action.DeleteJobAction;
import org.elasticsearch.xpack.prelert.job.Job;

import java.io.IOException;

public class RestDeleteJobAction extends BaseRestHandler {

    private final DeleteJobAction.TransportAction transportDeleteJobAction;

    @Inject
    public RestDeleteJobAction(Settings settings, RestController controller, DeleteJobAction.TransportAction transportDeleteJobAction) {
        super(settings);
        this.transportDeleteJobAction = transportDeleteJobAction;
        controller.registerHandler(RestRequest.Method.DELETE, PrelertPlugin.BASE_PATH + "jobs/{" + Job.ID.getPreferredName() + "}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        DeleteJobAction.Request deleteJobRequest = new DeleteJobAction.Request(restRequest.param(Job.ID.getPreferredName()));
        return channel -> transportDeleteJobAction.execute(deleteJobRequest, new AcknowledgedRestListener<>(channel));
    }
}
