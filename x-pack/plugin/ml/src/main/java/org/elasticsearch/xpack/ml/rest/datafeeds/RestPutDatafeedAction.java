/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutDatafeedAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestPutDatafeedAction.class));

    public RestPutDatafeedAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
            PUT, MachineLearning.BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}", this,
            PUT, MachineLearning.PRE_V7_BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}", deprecationLogger);
    }

    @Override
    public String getName() {
        return "ml_put_datafeed_action";
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
