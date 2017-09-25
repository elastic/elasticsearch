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

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetTrialStatus extends XPackRestHandler {

    RestGetTrialStatus(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, URI_BASE + "/license/trial_status", this);
    }

    @Override
    protected RestChannelConsumer doPrepareRequest(RestRequest request, XPackClient client) throws IOException {
        return channel -> client.licensing().prepareGetUpgradeToTrial().execute(
                new RestBuilderListener<GetTrialStatusResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(GetTrialStatusResponse response, XContentBuilder builder) throws Exception {
                        builder.startObject();
                        builder.field("eligible_to_start_trial", response.isEligibleToStartTrial());
                        builder.endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
    }

    @Override
    public String getName() {
        return "xpack_trial_status_action";
    }
}
