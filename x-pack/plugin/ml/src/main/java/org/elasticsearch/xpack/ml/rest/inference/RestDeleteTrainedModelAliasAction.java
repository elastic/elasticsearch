/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAliasAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAliasAction.Request.MODEL_ALIAS;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

public class RestDeleteTrainedModelAliasAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(DELETE, BASE_PATH + "trained_models/{" + TrainedModelConfig.MODEL_ID + "}/model_aliases/{" + MODEL_ALIAS + "}")
        );
    }

    @Override
    public String getName() {
        return "ml_delete_trained_model_alias_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final String modelId = restRequest.param(TrainedModelConfig.MODEL_ID.getPreferredName());
        final String modelAlias = restRequest.param(MODEL_ALIAS);
        return channel -> client.execute(
            DeleteTrainedModelAliasAction.INSTANCE,
            new DeleteTrainedModelAliasAction.Request(modelAlias, modelId),
            new RestToXContentListener<>(channel)
        );
    }
}
