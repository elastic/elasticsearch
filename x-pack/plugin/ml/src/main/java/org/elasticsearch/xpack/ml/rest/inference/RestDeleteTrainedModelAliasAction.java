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
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteTrainedModelAliasAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(
            new Route(
                DELETE,
                MachineLearning.BASE_PATH
                    + "trained_models/{"
                    + TrainedModelConfig.MODEL_ID.getPreferredName()
                    + "}/model_aliases/{"
                    + DeleteTrainedModelAliasAction.Request.MODEL_ALIAS
                    + "}"

            )
        );
    }

    @Override
    public String getName() {
        return "ml_delete_trained_model_alias_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final String modelId = restRequest.param(TrainedModelConfig.MODEL_ID.getPreferredName());
        final String modelAlias = restRequest.param(DeleteTrainedModelAliasAction.Request.MODEL_ALIAS);
        return channel -> client.execute(
            DeleteTrainedModelAliasAction.INSTANCE,
            new DeleteTrainedModelAliasAction.Request(modelAlias, modelId),
            new RestToXContentListener<>(channel)
        );
    }
}
