/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction.Request;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction.Response;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStopDatafeedAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(POST, MachineLearning.BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}/_stop")
        );
    }

    @Override
    public String getName() {
        return "ml_stop_datafeed_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        Request request;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            request = Request.parseRequest(datafeedId, parser);
        } else {
            request = new Request(datafeedId);
            if (restRequest.hasParam(Request.TIMEOUT.getPreferredName())) {
                TimeValue stopTimeout = restRequest.paramAsTime(Request.TIMEOUT.getPreferredName(), StopDatafeedAction.DEFAULT_TIMEOUT);
                request.setStopTimeout(stopTimeout);
            }
            if (restRequest.hasParam(Request.FORCE.getPreferredName())) {
                request.setForce(restRequest.paramAsBoolean(Request.FORCE.getPreferredName(), request.isForce()));
            }
            if (restRequest.hasParam(Request.ALLOW_NO_DATAFEEDS)) {
                LoggingDeprecationHandler.INSTANCE.usedDeprecatedName(
                    null, () -> null, Request.ALLOW_NO_DATAFEEDS, Request.ALLOW_NO_MATCH.getPreferredName());
            }
            request.setAllowNoMatch(
                restRequest.paramAsBoolean(
                    Request.ALLOW_NO_MATCH.getPreferredName(),
                    restRequest.paramAsBoolean(Request.ALLOW_NO_DATAFEEDS, request.allowNoMatch())));
        }
        return channel -> client.execute(StopDatafeedAction.INSTANCE, request, new RestBuilderListener<Response>(channel) {

            @Override
            public RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field("stopped", response.isStopped());
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
