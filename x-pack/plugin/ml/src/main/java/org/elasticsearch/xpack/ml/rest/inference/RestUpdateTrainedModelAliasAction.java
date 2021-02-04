/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.inference;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAliasAction;
import org.elasticsearch.xpack.ml.MachineLearning;

public class RestUpdateTrainedModelAliasAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(
            new Route(
                POST,
                MachineLearning.BASE_PATH
                    + "trained_models/model_aliases/{"
                    + UpdateTrainedModelAliasAction.Request.MODEL_ALIAS.getPreferredName()
                    + "}/_update"

            )
        );
    }

    @Override
    public String getName() {
        return "ml_update_trained_model_alias_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String modelAlias = restRequest.param(UpdateTrainedModelAliasAction.Request.MODEL_ALIAS.getPreferredName());
        XContentParser parser = restRequest.contentParser();
        UpdateTrainedModelAliasAction.Request request = UpdateTrainedModelAliasAction.Request.fromXContent(modelAlias, parser);
        return channel -> client.execute(UpdateTrainedModelAliasAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
