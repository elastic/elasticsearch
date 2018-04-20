/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

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

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestPostStartTrialLicense extends XPackRestHandler {

    RestPostStartTrialLicense(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, URI_BASE + "/license/start_trial", this);
    }

    @Override
    protected RestChannelConsumer doPrepareRequest(RestRequest request, XPackClient client) throws IOException {
        PostStartTrialRequest startTrialRequest = new PostStartTrialRequest();
        startTrialRequest.setType(request.param("type", "trial"));
        return channel -> client.licensing().postStartTrial(startTrialRequest,
                new RestBuilderListener<PostStartTrialResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(PostStartTrialResponse response, XContentBuilder builder) throws Exception {
                        PostStartTrialResponse.Status status = response.getStatus();
                        if (status.isTrialStarted()) {
                            builder.startObject()
                                    .field("trial_was_started", true)
                                    .field("type", startTrialRequest.getType())
                                    .endObject();
                        } else {
                            builder.startObject()
                                    .field("trial_was_started", false)
                                    .field("error_message", status.getErrorMessage())
                                    .endObject();

                        }
                        return new BytesRestResponse(status.getRestStatus(), builder);
                    }
                });
    }

    @Override
    public String getName() {
        return "xpack_upgrade_to_trial_action";
    }
}
