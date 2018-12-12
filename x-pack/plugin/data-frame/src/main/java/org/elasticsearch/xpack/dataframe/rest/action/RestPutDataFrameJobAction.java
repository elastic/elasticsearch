/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.dataframe.action.PutDataFrameJobAction;

import java.io.IOException;

public class RestPutDataFrameJobAction extends BaseRestHandler {
    
    public RestPutDataFrameJobAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.PUT, DataFrameField.REST_BASE_PATH_JOBS_BY_ID, this);
    }

    @Override
    public String getName() {
        return "data_frame_put_job_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(DataFrameField.ID.getPreferredName());
        XContentParser parser = restRequest.contentParser();

        PutDataFrameJobAction.Request request = PutDataFrameJobAction.Request.fromXContent(parser, id);

        return channel -> client.execute(PutDataFrameJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
