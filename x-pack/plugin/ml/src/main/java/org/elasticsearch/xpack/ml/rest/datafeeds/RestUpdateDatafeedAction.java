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
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;
import static org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig.ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestUpdateDatafeedAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, BASE_PATH + "datafeeds/{" + ID + "}/_update"));
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
        UpdateDatafeedAction.Request updateDatafeedRequest;
        try (XContentParser parser = restRequest.contentParser()) {
            updateDatafeedRequest = UpdateDatafeedAction.Request.parseRequest(datafeedId, indicesOptions, parser);
        }
        updateDatafeedRequest.ackTimeout(getAckTimeout(restRequest));
        updateDatafeedRequest.masterNodeTimeout(getMasterNodeTimeout(restRequest));

        return channel -> client.execute(UpdateDatafeedAction.INSTANCE, updateDatafeedRequest, new RestToXContentListener<>(channel));
    }

}
