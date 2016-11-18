/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.data;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.PostDataCloseAction;

import java.io.IOException;

public class RestPostDataCloseAction extends BaseRestHandler {

    private static final ParseField JOB_ID = new ParseField("jobId");

    private final PostDataCloseAction.TransportAction transportPostDataCloseAction;

    @Inject
    public RestPostDataCloseAction(Settings settings, RestController controller,
            PostDataCloseAction.TransportAction transportPostDataCloseAction) {
        super(settings);
        this.transportPostDataCloseAction = transportPostDataCloseAction;
        controller.registerHandler(RestRequest.Method.POST, PrelertPlugin.BASE_PATH + "data/{jobId}/_close", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PostDataCloseAction.Request postDataCloseRequest = new PostDataCloseAction.Request(restRequest.param(JOB_ID.getPreferredName()));

        return channel -> transportPostDataCloseAction.execute(postDataCloseRequest, new AcknowledgedRestListener<>(channel));
    }
}
