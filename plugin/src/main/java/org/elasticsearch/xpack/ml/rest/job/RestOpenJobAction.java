/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.persistent.PersistentActionResponse;

import java.io.IOException;

public class RestOpenJobAction extends BaseRestHandler {

    public RestOpenJobAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.BASE_PATH
                + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_open", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        OpenJobAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            request = OpenJobAction.Request.parseRequest(restRequest.param(Job.ID.getPreferredName()), restRequest.contentParser());
        } else {
            request = new OpenJobAction.Request(restRequest.param(Job.ID.getPreferredName()));
            request.setIgnoreDowntime(restRequest.paramAsBoolean(OpenJobAction.Request.IGNORE_DOWNTIME.getPreferredName(), true));
            if (restRequest.hasParam("timeout")) {
                TimeValue openTimeout = restRequest.paramAsTime("timeout", TimeValue.timeValueSeconds(20));
                request.setTimeout(openTimeout);
            }
        }
        return channel -> {
            client.execute(OpenJobAction.INSTANCE, request, new RestBuilderListener<PersistentActionResponse>(channel) {

                @Override
                public RestResponse buildResponse(PersistentActionResponse r, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    builder.field("opened", true);
                    builder.endObject();
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
        };
    }
}
