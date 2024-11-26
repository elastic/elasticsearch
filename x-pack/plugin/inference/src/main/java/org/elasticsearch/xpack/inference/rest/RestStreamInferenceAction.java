/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.inference.rest.Paths.STREAM_INFERENCE_ID_PATH;
import static org.elasticsearch.xpack.inference.rest.Paths.STREAM_TASK_TYPE_INFERENCE_ID_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestStreamInferenceAction extends BaseInferenceAction {
    @Override
    public String getName() {
        return "stream_inference_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, STREAM_INFERENCE_ID_PATH), new Route(POST, STREAM_TASK_TYPE_INFERENCE_ID_PATH));
    }

    @Override
    protected InferenceAction.Request prepareInferenceRequest(InferenceAction.Request.Builder builder) {
        return builder.setStream(true).build();
    }

    @Override
    protected ActionListener<InferenceAction.Response> listener(RestChannel channel) {
        return new ServerSentEventsRestActionListener(channel);
    }
}
