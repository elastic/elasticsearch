/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

public class RestOpenJobAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/_open")
                .replaces(POST, PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID + "}/_open", RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "ml_open_job_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        OpenJobAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            request = OpenJobAction.Request.parseRequest(restRequest.param(Job.ID.getPreferredName()), restRequest.contentParser());
        } else {
            OpenJobAction.JobParams jobParams = new OpenJobAction.JobParams(restRequest.param(Job.ID.getPreferredName()));
            if (restRequest.hasParam(OpenJobAction.JobParams.TIMEOUT.getPreferredName())) {
                TimeValue openTimeout = restRequest.paramAsTime(
                    OpenJobAction.JobParams.TIMEOUT.getPreferredName(),
                    TimeValue.timeValueSeconds(20)
                );
                jobParams.setTimeout(openTimeout);
            }
            request = new OpenJobAction.Request(jobParams);
        }
        return channel -> {
            client.execute(OpenJobAction.INSTANCE, request, new RestBuilderListener<NodeAcknowledgedResponse>(channel) {
                @Override
                public RestResponse buildResponse(NodeAcknowledgedResponse r, XContentBuilder builder) throws Exception {
                    // This doesn't use the toXContent of the response object because we rename "acknowledged" to "opened"
                    builder.startObject();
                    builder.field("opened", r.isAcknowledged());
                    builder.field(NodeAcknowledgedResponse.NODE_FIELD, r.getNode());
                    builder.endObject();
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
        };
    }
}
