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
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction.Request;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction.Response;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.ml.MachineLearningField.DEPRECATED_ALLOW_NO_DATAFEEDS_PARAM;
import static org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig.ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.rest.RestCompatibilityChecker.checkAndSetDeprecatedParam;

@ServerlessScope(Scope.PUBLIC)
public class RestStopDatafeedAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, BASE_PATH + "datafeeds/{" + ID + "}/_stop"));
    }

    @Override
    public String getName() {
        return "ml_stop_datafeed_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        @UpdateForV9(owner = UpdateForV9.Owner.MACHINE_LEARNING) // v7 REST API no longer exists: eliminate ref to RestApiVersion.V_7
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
            checkAndSetDeprecatedParam(
                DEPRECATED_ALLOW_NO_DATAFEEDS_PARAM,
                Request.ALLOW_NO_MATCH.getPreferredName(),
                RestApiVersion.V_7,
                restRequest,
                (r, s) -> r.paramAsBoolean(s, request.allowNoMatch()),
                request::setAllowNoMatch
            );
        }
        return channel -> client.execute(StopDatafeedAction.INSTANCE, request, new RestBuilderListener<Response>(channel) {

            @Override
            public RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field("stopped", response.isStopped());
                builder.endObject();
                return new RestResponse(RestStatus.OK, builder);
            }
        });
    }
}
