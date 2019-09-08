/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.rest.action;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformAction;

public class RestStartDataFrameTransformAction extends BaseRestHandler {

    public RestStartDataFrameTransformAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.POST, DataFrameField.REST_BASE_PATH_TRANSFORMS_BY_ID + "_start", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(DataFrameField.ID.getPreferredName());
        boolean force = restRequest.paramAsBoolean(DataFrameField.FORCE.getPreferredName(), false);
        StartDataFrameTransformAction.Request request = new StartDataFrameTransformAction.Request(id, force);
        request.timeout(restRequest.paramAsTime(DataFrameField.TIMEOUT.getPreferredName(), AcknowledgedRequest.DEFAULT_ACK_TIMEOUT));
        return channel -> client.execute(StartDataFrameTransformAction.INSTANCE, request,
                new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "data_frame_start_transform_action";
    }
}
