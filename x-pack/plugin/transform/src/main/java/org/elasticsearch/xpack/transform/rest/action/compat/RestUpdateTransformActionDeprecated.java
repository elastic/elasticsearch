/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.rest.action.compat;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;
import org.elasticsearch.xpack.core.transform.action.compat.UpdateTransformActionDeprecated;

import java.io.IOException;

public class RestUpdateTransformActionDeprecated extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
            LogManager.getLogger(RestUpdateTransformActionDeprecated.class));

    public RestUpdateTransformActionDeprecated(RestController controller) {
        controller.registerAsDeprecatedHandler(RestRequest.Method.POST,
                TransformField.REST_BASE_PATH_TRANSFORMS_BY_ID_DEPRECATED + "_update", this, TransformMessages.REST_DEPRECATED_ENDPOINT,
                deprecationLogger);
    }

    @Override
    public String getName() {
        return "data_frame_update_transform_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(TransformField.ID.getPreferredName());
        boolean deferValidation = restRequest.paramAsBoolean(TransformField.DEFER_VALIDATION.getPreferredName(), false);
        XContentParser parser = restRequest.contentParser();
        UpdateTransformAction.Request request = UpdateTransformAction.Request.fromXContent(parser, id, deferValidation);

        return channel -> client.execute(UpdateTransformActionDeprecated.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
