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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.XPackClient;
import org.elasticsearch.xpack.rest.XPackRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPostStartTrialLicense extends XPackRestHandler {

    RestPostStartTrialLicense(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, URI_BASE + "/license/start_trial", this);
    }

    @Override
    protected RestChannelConsumer doPrepareRequest(RestRequest request, XPackClient client) throws IOException {
        return channel -> client.licensing().preparePutUpgradeToTrial().execute(
                new RestBuilderListener<PostStartTrialResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(PostStartTrialResponse response, XContentBuilder builder) throws Exception {
                        PostStartTrialResponse.STATUS status = response.getStatus();
                        if (status == PostStartTrialResponse.STATUS.TRIAL_ALREADY_ACTIVATED) {
                            builder.startObject()
                                    .field("trial_was_started", false)
                                    .field("error_message", "Operation failed: Trial was already activated.")
                                    .endObject();
                            return new BytesRestResponse(RestStatus.FORBIDDEN, builder);
                        } else if (status == PostStartTrialResponse.STATUS.UPGRADED_TO_TRIAL) {
                            builder.startObject().field("trial_was_started", true).endObject();
                            return new BytesRestResponse(RestStatus.OK, builder);
                        } else {
                            throw new IllegalArgumentException("Unexpected status for PostStartTrialResponse: [" + status + "]");
                        }
                    }
                });
    }

    @Override
    public String getName() {
        return "xpack_upgrade_to_trial_action";
    }
}
