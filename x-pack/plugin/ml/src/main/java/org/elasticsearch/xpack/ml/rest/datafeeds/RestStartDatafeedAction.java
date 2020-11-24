/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStartDatafeedAction extends BaseRestHandler {

    private static final String DEFAULT_START = "0";

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(POST, MachineLearning.BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}/_start")
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
                        StartDatafeedAction.TIMEOUT.getPreferredName(), TimeValue.timeValueSeconds(20));
                datafeedParams.setTimeout(openTimeout);
            }
            jobDatafeedRequest = new StartDatafeedAction.Request(datafeedParams);
        }
        return channel -> {
            client.execute(StartDatafeedAction.INSTANCE, jobDatafeedRequest,
                    new RestBuilderListener<NodeAcknowledgedResponse>(channel) {

                        @Override
                        public RestResponse buildResponse(NodeAcknowledgedResponse r, XContentBuilder builder) throws Exception {
                            // This doesn't use the toXContent of the response object because we rename "acknowledged" to "started"
                            builder.startObject();
                            builder.field("started", r.isAcknowledged());
                            builder.field(NodeAcknowledgedResponse.NODE_FIELD, r.getNode());
                            builder.endObject();
                            return new BytesRestResponse(RestStatus.OK, builder);
                        }
                    });
        };
    }
}
