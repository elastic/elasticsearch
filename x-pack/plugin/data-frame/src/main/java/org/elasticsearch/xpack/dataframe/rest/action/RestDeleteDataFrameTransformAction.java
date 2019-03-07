/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.rest.action;


import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.action.DeleteDataFrameTransformAction;

import java.io.IOException;

public class RestDeleteDataFrameTransformAction extends BaseRestHandler {

    public RestDeleteDataFrameTransformAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.DELETE,  DataFrameField.REST_BASE_PATH_TRANSFORMS_BY_ID, this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        if (restRequest.hasContent()) {
            throw new IllegalArgumentException("delete data frame transforms requests can not have a request body");
        }

        String id = restRequest.param(DataFrameField.ID.getPreferredName());
        DeleteDataFrameTransformAction.Request request = new DeleteDataFrameTransformAction.Request(id);

        return channel -> client.execute(DeleteDataFrameTransformAction.INSTANCE, request,
                new RestToXContentListener<DeleteDataFrameTransformAction.Response>(channel) {
                    @Override
                    protected RestStatus getStatus(DeleteDataFrameTransformAction.Response response) {
                        if (response.getNodeFailures().size() > 0 || response.getTaskFailures().size() > 0) {
                            return RestStatus.INTERNAL_SERVER_ERROR;
                        }
                        return RestStatus.OK;
                    }
                });
    }

    @Override
    public String getName() {
        return "data_frame_delete_transform_action";
    }
}
