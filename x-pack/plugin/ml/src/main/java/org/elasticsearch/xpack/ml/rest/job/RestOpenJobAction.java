/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestOpenJobAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(POST, MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_open")
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
                TimeValue openTimeout = restRequest.paramAsTime(OpenJobAction.JobParams.TIMEOUT.getPreferredName(),
                        TimeValue.timeValueSeconds(20));
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
