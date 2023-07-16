/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestPostStartTrialLicense extends BaseRestHandler {

    public RestPostStartTrialLicense() {}

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, "/_license/start_trial").replaces(POST, "/_xpack/license/start_trial", RestApiVersion.V_7).build()
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        PostStartTrialRequest startTrialRequest = new PostStartTrialRequest();
        startTrialRequest.setType(request.param("type", License.LicenseType.TRIAL.getTypeName()));
        startTrialRequest.acknowledge(request.paramAsBoolean("acknowledge", false));
        return channel -> client.execute(PostStartTrialAction.INSTANCE, startTrialRequest, new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(PostStartTrialResponse response, XContentBuilder builder) throws Exception {
                PostStartTrialResponse.Status status = response.getStatus();
                builder.startObject();
                builder.field("acknowledged", startTrialRequest.isAcknowledged());
                if (status.isTrialStarted()) {
                    builder.field("trial_was_started", true);
                    builder.field("type", startTrialRequest.getType());
                } else {
                    builder.field("trial_was_started", false);
                    builder.field("error_message", status.getErrorMessage());
                }

                Map<String, String[]> acknowledgementMessages = response.getAcknowledgementMessages();
                if (acknowledgementMessages.isEmpty() == false) {
                    builder.startObject("acknowledge");
                    builder.field("message", response.getAcknowledgementMessage());
                    for (Map.Entry<String, String[]> entry : acknowledgementMessages.entrySet()) {
                        builder.startArray(entry.getKey());
                        for (String message : entry.getValue()) {
                            builder.value(message);
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
                return new RestResponse(status.getRestStatus(), builder);
            }
        });
    }

    @Override
    public String getName() {
        return "post_start_trial";
    }

}
