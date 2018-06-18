/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.indexlifecycle.OperationMode;
import org.elasticsearch.xpack.core.indexlifecycle.action.SetOperationModeAction;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycle;

public class RestStopMaintenanceAction extends BaseRestHandler {

    public RestStopMaintenanceAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST,
            IndexLifecycle.BASE_PATH + "maintenance/_stop", this);
    }

    @Override
    public String getName() {
        return "xpack_lifecycle_stop_maintenance_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        SetOperationModeAction.Request request = new SetOperationModeAction.Request(OperationMode.NORMAL);
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
        return channel -> client.execute(SetOperationModeAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
