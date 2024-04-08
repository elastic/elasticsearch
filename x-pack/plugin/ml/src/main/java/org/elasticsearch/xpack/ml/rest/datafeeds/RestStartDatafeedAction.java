/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestStartDatafeedAction extends BaseRestHandler {

    private static final String DEFAULT_START = "0";

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, BASE_PATH + "datafeeds/{" + DatafeedConfig.ID + "}/_start")
                .replaces(POST, PRE_V7_BASE_PATH + "datafeeds/{" + DatafeedConfig.ID + "}/_start", RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "ml_start_datafeed_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        StartDatafeedAction.Request jobDatafeedRequest;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            jobDatafeedRequest = StartDatafeedAction.Request.parseRequest(datafeedId, parser);
        } else {
            String startTime = restRequest.param(StartDatafeedAction.START_TIME.getPreferredName(), DEFAULT_START);
            StartDatafeedAction.DatafeedParams datafeedParams = new StartDatafeedAction.DatafeedParams(datafeedId, startTime);
            if (restRequest.hasParam(StartDatafeedAction.END_TIME.getPreferredName())) {
                datafeedParams.setEndTime(restRequest.param(StartDatafeedAction.END_TIME.getPreferredName()));
            }
            if (restRequest.hasParam(StartDatafeedAction.TIMEOUT.getPreferredName())) {
                TimeValue openTimeout = restRequest.paramAsTime(
                    StartDatafeedAction.TIMEOUT.getPreferredName(),
                    TimeValue.timeValueSeconds(20)
                );
                datafeedParams.setTimeout(openTimeout);
            }
            jobDatafeedRequest = new StartDatafeedAction.Request(datafeedParams);
        }
        return channel -> {
            client.execute(StartDatafeedAction.INSTANCE, jobDatafeedRequest, new RestBuilderListener<NodeAcknowledgedResponse>(channel) {

                @Override
                public RestResponse buildResponse(NodeAcknowledgedResponse r, XContentBuilder builder) throws Exception {
                    // This doesn't use the toXContent of the response object because we rename "acknowledged" to "started"
                    builder.startObject();
                    builder.field("started", r.isAcknowledged());
                    builder.field(NodeAcknowledgedResponse.NODE_FIELD, r.getNode());
                    builder.endObject();
                    return new RestResponse(RestStatus.OK, builder);
                }
            });
        };
    }
}
