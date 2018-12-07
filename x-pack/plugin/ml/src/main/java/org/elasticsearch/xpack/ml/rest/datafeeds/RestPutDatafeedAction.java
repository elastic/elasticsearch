/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.io.IOException;

public class RestPutDatafeedAction extends BaseRestHandler {

    public RestPutDatafeedAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.PUT, MachineLearning.BASE_PATH + "datafeeds/{"
                + DatafeedConfig.ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.PUT, MachineLearning.V7_BASE_PATH + "datafeeds/{"
                + DatafeedConfig.ID.getPreferredName() + "}", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_put_datafeed_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        XContentParser parser = restRequest.contentParser();
        PutDatafeedAction.Request putDatafeedRequest = PutDatafeedAction.Request.parseRequest(datafeedId, parser);
        putDatafeedRequest.timeout(restRequest.paramAsTime("timeout", putDatafeedRequest.timeout()));
        putDatafeedRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", putDatafeedRequest.masterNodeTimeout()));
        return channel -> client.execute(PutDatafeedAction.INSTANCE, putDatafeedRequest, new RestToXContentListener<>(channel));
    }

}
