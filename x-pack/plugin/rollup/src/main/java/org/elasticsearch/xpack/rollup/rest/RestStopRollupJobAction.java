/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.rollup.rest;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.StopRollupJobAction;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStopRollupJobAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestStopRollupJobAction.class));

    public RestStopRollupJobAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                POST, "/_rollup/job/{id}/_stop", this,
                POST, "/_xpack/rollup/job/{id}/_stop", deprecationLogger);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(RollupField.ID.getPreferredName());
        TimeValue timeout = restRequest.paramAsTime(StopRollupJobAction.TIMEOUT.getPreferredName(), StopRollupJobAction.DEFAULT_TIMEOUT);
        boolean waitForCompletion = restRequest.paramAsBoolean(StopRollupJobAction.WAIT_FOR_COMPLETION.getPreferredName(), false);
        StopRollupJobAction.Request request = new StopRollupJobAction.Request(id, waitForCompletion, timeout);

        return channel -> client.execute(StopRollupJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "stop_rollup_job";
    }

}
