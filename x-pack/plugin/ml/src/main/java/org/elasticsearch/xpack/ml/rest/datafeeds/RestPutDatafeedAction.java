/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutDatafeedAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(PUT, MachineLearning.BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}")
        );
    }

    @Override
    public String getName() {
        return "ml_put_datafeed_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        IndicesOptions indicesOptions = IndicesOptions.fromRequest(restRequest, SearchRequest.DEFAULT_INDICES_OPTIONS);
        XContentParser parser = restRequest.contentParser();
        PutDatafeedAction.Request putDatafeedRequest = PutDatafeedAction.Request.parseRequest(datafeedId, indicesOptions, parser);
        putDatafeedRequest.timeout(restRequest.paramAsTime("timeout", putDatafeedRequest.timeout()));
        putDatafeedRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", putDatafeedRequest.masterNodeTimeout()));
        return channel -> client.execute(PutDatafeedAction.INSTANCE, putDatafeedRequest, new RestToXContentListener<>(channel));
    }

}
