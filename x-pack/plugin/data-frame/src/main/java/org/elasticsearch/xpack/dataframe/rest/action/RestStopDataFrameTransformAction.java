/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.action.StopDataFrameTransformAction;

public class RestStopDataFrameTransformAction extends BaseRestHandler {

    public RestStopDataFrameTransformAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.POST, DataFrameField.REST_BASE_PATH_TRANSFORMS_BY_ID + "_stop", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(DataFrameField.ID.getPreferredName());
        TimeValue timeout = restRequest.paramAsTime(DataFrameField.TIMEOUT.getPreferredName(),
                StopDataFrameTransformAction.DEFAULT_TIMEOUT);
        boolean waitForCompletion = restRequest.paramAsBoolean(DataFrameField.WAIT_FOR_COMPLETION.getPreferredName(), false);
        boolean force = restRequest.paramAsBoolean(DataFrameField.FORCE.getPreferredName(), false);
        boolean allowNoMatch = restRequest.paramAsBoolean(DataFrameField.ALLOW_NO_MATCH.getPreferredName(), false);


        StopDataFrameTransformAction.Request request = new StopDataFrameTransformAction.Request(id,
            waitForCompletion,
            force,
            timeout,
            allowNoMatch);

        return channel -> client.execute(StopDataFrameTransformAction.INSTANCE, request,
                new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "data_frame_stop_transform_action";
    }
}
