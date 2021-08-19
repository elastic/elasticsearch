/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.rest.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestPreviewTransformAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, TransformField.REST_BASE_PATH_TRANSFORMS + "_preview"),
            new Route(POST, TransformField.REST_BASE_PATH_TRANSFORMS_BY_ID + "_preview"));
    }

    @Override
    public String getName() {
        return "transform_preview_transform_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        boolean hasId = restRequest.hasParam(TransformField.ID.getPreferredName());
        boolean hasContent = restRequest.hasContent();
        PreviewTransformAction.Request request;
        if (hasId && hasContent) {
            String idFromParam = restRequest.param(TransformField.ID.getPreferredName());
            String idFromBody = (String) restRequest.contentParser().map().get(TransformField.ID.getPreferredName());
            if (idFromParam.equals(idFromBody) == false) {
                throw new ElasticsearchParseException(
                    "transform id param [" + idFromParam + "] does not match request body id [" + idFromBody + "]");
            }
            XContentParser parser = restRequest.contentParser();
            request = PreviewTransformAction.Request.fromXContent(parser);
        } else if (hasId) {
            String id = restRequest.param(TransformField.ID.getPreferredName());
            request = new PreviewTransformAction.Request(id);
        } else if (hasContent) {
            XContentParser parser = restRequest.contentParser();
            request = PreviewTransformAction.Request.fromXContent(parser);
        } else {
            throw new ElasticsearchParseException("Either transform id param or request body is required");
        }
        return channel -> client.execute(PreviewTransformAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
