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
import org.elasticsearch.rest.action.RestChunkedToXContentListener;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_ID_PATH;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE_INFERENCE_ID_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestInferenceAction extends BaseInferenceAction {
    @Override
    public String getName() {
        return "inference_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, INFERENCE_ID_PATH), new Route(POST, TASK_TYPE_INFERENCE_ID_PATH));
    }

    @Override
    protected ActionListener<InferenceAction.Response> listener(RestChannel channel) {
        return new RestChunkedToXContentListener<>(channel);
    }
}
