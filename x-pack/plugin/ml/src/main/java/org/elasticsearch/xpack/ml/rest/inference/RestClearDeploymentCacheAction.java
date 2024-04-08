/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.ClearDeploymentCacheAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestClearDeploymentCacheAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "xpack_ml_clear_deployment_cache_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(POST, BASE_PATH + "trained_models/{" + TrainedModelConfig.MODEL_ID.getPreferredName() + "}/deployment/cache/_clear")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String modelId = restRequest.param(TrainedModelConfig.MODEL_ID.getPreferredName());
        return channel -> client.execute(
            ClearDeploymentCacheAction.INSTANCE,
            new ClearDeploymentCacheAction.Request(modelId),
            new RestToXContentListener<>(channel)
        );
    }
}
