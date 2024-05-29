/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestMoveToStepAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_ilm/move/{name}"));
    }

    @Override
    public String getName() {
        return "ilm_move_to_step_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final var masterNodeTimeout = getMasterNodeTimeout(restRequest);
        final var ackTimeout = getAckTimeout(restRequest);
        final var index = restRequest.param("name");
        final TransportMoveToStepAction.Request request;
        try (XContentParser parser = restRequest.contentParser()) {
            request = TransportMoveToStepAction.Request.parseRequest(
                (currentStepKey, nextStepKey) -> new TransportMoveToStepAction.Request(
                    masterNodeTimeout,
                    ackTimeout,
                    index,
                    currentStepKey,
                    nextStepKey
                ),
                parser
            );
        }
        return channel -> client.execute(ILMActions.MOVE_TO_STEP, request, new RestToXContentListener<>(channel));
    }
}
