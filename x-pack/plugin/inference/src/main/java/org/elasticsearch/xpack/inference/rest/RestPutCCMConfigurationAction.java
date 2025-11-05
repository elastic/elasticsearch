/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.inference.action.PutCCMConfigurationAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_CCM_PATH;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE_INFERENCE_ID_PATH;

public class RestPutCCMConfigurationAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "put_inference_ccm_configuration_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, INFERENCE_CCM_PATH), new Route(PUT, TASK_TYPE_INFERENCE_ID_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        if (restRequest.hasContent() == false) {
            throw new IllegalArgumentException("The api key must be provided in the request body");
        }

        final PutCCMConfigurationAction.Request putReq;
        try (var parser = restRequest.contentParser()) {
            putReq = PutCCMConfigurationAction.Request.parseRequest(
                RestUtils.getMasterNodeTimeout(restRequest),
                RestUtils.getAckTimeout(restRequest),
                parser
            );
        }

        return channel -> client.execute(PutInferenceModelAction.INSTANCE, putReq, new RestToXContentListener<>(channel));
    }
}
