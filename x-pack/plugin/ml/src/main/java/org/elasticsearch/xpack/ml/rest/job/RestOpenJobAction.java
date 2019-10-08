/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestOpenJobAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestOpenJobAction.class));

    public RestOpenJobAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
            POST, MachineLearning.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_open", this,
            POST, MachineLearning.PRE_V7_BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_open", deprecationLogger);
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
            client.execute(OpenJobAction.INSTANCE, request, new RestBuilderListener<AcknowledgedResponse>(channel) {
                @Override
                public RestResponse buildResponse(AcknowledgedResponse r, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    builder.field("opened", r.isAcknowledged());
                    builder.endObject();
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
        };
    }
}
