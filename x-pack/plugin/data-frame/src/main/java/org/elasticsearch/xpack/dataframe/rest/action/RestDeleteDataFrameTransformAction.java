/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.rest.action;


import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.action.DeleteDataFrameTransformAction;

public class RestDeleteDataFrameTransformAction extends BaseRestHandler {

    public RestDeleteDataFrameTransformAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.DELETE,  DataFrameField.REST_BASE_PATH_TRANSFORMS_BY_ID, this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        if (restRequest.hasContent()) {
            throw new IllegalArgumentException("delete data frame transforms requests can not have a request body");
        }

        String id = restRequest.param(DataFrameField.ID.getPreferredName());
        boolean force = restRequest.paramAsBoolean(DataFrameField.FORCE.getPreferredName(), false);
        DeleteDataFrameTransformAction.Request request = new DeleteDataFrameTransformAction.Request(id, force);

        return channel -> client.execute(DeleteDataFrameTransformAction.INSTANCE, request,
                new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "data_frame_delete_transform_action";
    }
}
