/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.transform.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStopTransformAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, TransformField.REST_BASE_PATH_TRANSFORMS_BY_ID + "_stop"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(TransformField.ID.getPreferredName());
        TimeValue timeout = restRequest.paramAsTime(TransformField.TIMEOUT.getPreferredName(),
                StopTransformAction.DEFAULT_TIMEOUT);
        boolean waitForCompletion = restRequest.paramAsBoolean(TransformField.WAIT_FOR_COMPLETION.getPreferredName(), false);
        boolean force = restRequest.paramAsBoolean(TransformField.FORCE.getPreferredName(), false);
        boolean allowNoMatch = restRequest.paramAsBoolean(TransformField.ALLOW_NO_MATCH.getPreferredName(), false);
        boolean waitForCheckpoint = restRequest.paramAsBoolean(TransformField.WAIT_FOR_CHECKPOINT.getPreferredName(), false);


        StopTransformAction.Request request = new StopTransformAction.Request(id,
            waitForCompletion,
            force,
            timeout,
            allowNoMatch,
            waitForCheckpoint);

        return channel -> client.execute(StopTransformAction.INSTANCE, request,
                new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "transform_stop_transform_action";
    }
}
