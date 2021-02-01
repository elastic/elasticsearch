/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.rest.action.compat;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.action.compat.GetTransformStatsActionDeprecated;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.core.transform.TransformField.ALLOW_NO_MATCH;

public class RestGetTransformStatsActionDeprecated extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<DeprecatedRoute> deprecatedRoutes() {
        return List.of(
            new DeprecatedRoute(GET, TransformField.REST_BASE_PATH_TRANSFORMS_DEPRECATED + "_stats",
                    TransformMessages.REST_DEPRECATED_ENDPOINT),
            new DeprecatedRoute(GET, TransformField.REST_BASE_PATH_TRANSFORMS_BY_ID_DEPRECATED + "_stats",
                    TransformMessages.REST_DEPRECATED_ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(TransformField.ID.getPreferredName());
        GetTransformStatsAction.Request request = new GetTransformStatsAction.Request(id);
        request.setAllowNoMatch(restRequest.paramAsBoolean(ALLOW_NO_MATCH.getPreferredName(), true));
        if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
            request.setPageParams(
                new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
        }
        return channel -> client.execute(GetTransformStatsActionDeprecated.INSTANCE, request,
                new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "data_frame_get_transforms_stats_action";
    }
}
