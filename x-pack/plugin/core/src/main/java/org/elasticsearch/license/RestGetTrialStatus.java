/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.license;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetTrialStatus extends BaseRestHandler {

    RestGetTrialStatus(RestController controller) {
        controller.registerHandler(GET, "/_license/trial_status", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> new GetTrialStatusRequestBuilder(client).execute(new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "get_trial_status";
    }

}
