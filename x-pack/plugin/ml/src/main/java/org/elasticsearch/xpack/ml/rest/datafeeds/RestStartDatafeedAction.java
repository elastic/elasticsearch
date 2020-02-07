/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStartDatafeedAction extends BaseRestHandler {

    private static final String DEFAULT_START = "0";

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestStartDatafeedAction.class));

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // TODO: remove deprecated endpoint in 8.0.0
        return Collections.singletonList(
            new ReplacedRoute(POST, MachineLearning.BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}/_start",
                POST, MachineLearning.PRE_V7_BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}/_start",
                deprecationLogger)
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
                    new RestBuilderListener<AcknowledgedResponse>(channel) {

                        @Override
                        public RestResponse buildResponse(AcknowledgedResponse r, XContentBuilder builder) throws Exception {
                            builder.startObject();
                            builder.field("started", r.isAcknowledged());
                            builder.endObject();
                            return new BytesRestResponse(RestStatus.OK, builder);
                        }
                    });
        };
    }
}
