/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.license;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestPostStartTrialLicense extends XPackRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestPostStartTrialLicense.class));

    RestPostStartTrialLicense(Settings settings, RestController controller) {
        super(settings);
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                POST, "/_license/start_trial", this,
                POST, URI_BASE + "/license/start_trial", deprecationLogger);
    }

    @Override
    protected RestChannelConsumer doPrepareRequest(RestRequest request, XPackClient client) throws IOException {
        PostStartTrialRequest startTrialRequest = new PostStartTrialRequest();
        startTrialRequest.setType(request.param("type", "trial"));
        startTrialRequest.acknowledge(request.paramAsBoolean("acknowledge", false));
        return channel -> client.licensing().postStartTrial(startTrialRequest,
                new RestBuilderListener<PostStartTrialResponse>(channel) {
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
                        return new BytesRestResponse(status.getRestStatus(), builder);
                    }
                });
    }

    @Override
    public String getName() {
        return "post_start_trial";
    }

}
