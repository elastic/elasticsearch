/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.inference;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAliasAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

public class RestPutTrainedModelAliasAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(
            new Route(
                PUT,
                MachineLearning.BASE_PATH
                    + "trained_models/{"
                    + TrainedModelConfig.MODEL_ID.getPreferredName()
                    + "}/model_aliases/{"
                    + PutTrainedModelAliasAction.Request.MODEL_ALIAS
                    + "}"

            )
        );
    }

    @Override
    public String getName() {
        return "ml_put_trained_model_alias_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String modelAlias = restRequest.param(PutTrainedModelAliasAction.Request.MODEL_ALIAS);
        String modelId = restRequest.param(TrainedModelConfig.MODEL_ID.getPreferredName());
        boolean reassign = restRequest.paramAsBoolean(PutTrainedModelAliasAction.Request.REASSIGN, false);
        return channel -> client.execute(
            PutTrainedModelAliasAction.INSTANCE,
            new PutTrainedModelAliasAction.Request(modelAlias, modelId, reassign),
            new RestToXContentListener<>(channel)
        );
    }
}
