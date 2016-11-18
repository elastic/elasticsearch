/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.data;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.PostDataAction;
import org.elasticsearch.xpack.prelert.job.Job;

import java.io.IOException;

public class RestPostDataAction extends BaseRestHandler {

    private static final boolean DEFAULT_IGNORE_DOWNTIME = false;
    private static final String DEFAULT_RESET_START = "";
    private static final String DEFAULT_RESET_END = "";

    private final PostDataAction.TransportAction transportPostDataAction;

    @Inject
    public RestPostDataAction(Settings settings, RestController controller, PostDataAction.TransportAction transportPostDataAction) {
        super(settings);
        this.transportPostDataAction = transportPostDataAction;
        controller.registerHandler(RestRequest.Method.POST, PrelertPlugin.BASE_PATH + "data/{jobId}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PostDataAction.Request request = new PostDataAction.Request(restRequest.param(Job.ID.getPreferredName()));
        request.setIgnoreDowntime(
                restRequest.paramAsBoolean(PostDataAction.Request.IGNORE_DOWNTIME.getPreferredName(), DEFAULT_IGNORE_DOWNTIME));
        request.setResetStart(restRequest.param(PostDataAction.Request.RESET_START.getPreferredName(), DEFAULT_RESET_START));
        request.setResetEnd(restRequest.param(PostDataAction.Request.RESET_END.getPreferredName(), DEFAULT_RESET_END));
        request.setContent(restRequest.content());

        return channel -> transportPostDataAction.execute(request, new RestStatusToXContentListener<>(channel));
    }
}