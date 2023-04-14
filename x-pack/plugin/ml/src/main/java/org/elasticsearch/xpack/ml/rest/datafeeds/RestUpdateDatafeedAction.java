/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestUpdateDatafeedAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, BASE_PATH + "datafeeds/{" + DatafeedConfig.ID + "}/_update")
                .replaces(POST, PRE_V7_BASE_PATH + "datafeeds/{" + DatafeedConfig.ID + "}/_update", RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "ml_update_datafeed_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        IndicesOptions indicesOptions = null;
        if (restRequest.hasParam("expand_wildcards")
            || restRequest.hasParam("ignore_unavailable")
            || restRequest.hasParam("allow_no_indices")
            || restRequest.hasParam("ignore_throttled")) {
            indicesOptions = IndicesOptions.fromRequest(restRequest, SearchRequest.DEFAULT_INDICES_OPTIONS);
        }
        XContentParser parser = restRequest.contentParser();
        UpdateDatafeedAction.Request updateDatafeedRequest = UpdateDatafeedAction.Request.parseRequest(datafeedId, indicesOptions, parser);
        updateDatafeedRequest.timeout(restRequest.paramAsTime("timeout", updateDatafeedRequest.timeout()));
        updateDatafeedRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", updateDatafeedRequest.masterNodeTimeout()));

        return channel -> client.execute(UpdateDatafeedAction.INSTANCE, updateDatafeedRequest, new RestToXContentListener<>(channel));
    }

}
