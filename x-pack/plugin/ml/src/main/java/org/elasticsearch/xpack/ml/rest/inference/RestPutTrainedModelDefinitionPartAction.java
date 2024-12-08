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
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction.Request.PART;
import static org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig.MODEL_ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestPutTrainedModelDefinitionPartAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, BASE_PATH + "trained_models/{" + MODEL_ID.getPreferredName() + "}/definition/{" + PART + "}"));
    }

    @Override
    public String getName() {
        return "xpack_ml_put_trained_model_definition_part_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(TrainedModelConfig.MODEL_ID.getPreferredName());
        int part = Integer.parseInt(restRequest.param(PutTrainedModelDefinitionPartAction.Request.PART));
        XContentParser parser = restRequest.contentParser();
        PutTrainedModelDefinitionPartAction.Request putRequest = PutTrainedModelDefinitionPartAction.Request.parseRequest(id, part, parser);
        return channel -> client.execute(PutTrainedModelDefinitionPartAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
    }
}
