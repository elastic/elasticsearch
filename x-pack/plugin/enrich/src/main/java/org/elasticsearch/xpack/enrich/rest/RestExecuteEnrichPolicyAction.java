/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;

import java.io.IOException;

public class RestExecuteEnrichPolicyAction extends BaseRestHandler {

    public RestExecuteEnrichPolicyAction(final RestController controller) {
        controller.registerHandler(RestRequest.Method.PUT, "/_enrich/policy/{name}/_execute", this);
        controller.registerHandler(RestRequest.Method.POST, "/_enrich/policy/{name}/_execute", this);
    }

    @Override
    public String getName() {
        return "execute_enrich_policy";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) throws IOException {
        final ExecuteEnrichPolicyAction.Request request = new ExecuteEnrichPolicyAction.Request(restRequest.param("name"));
        request.setWaitForCompletion(restRequest.paramAsBoolean("wait_for_completion", true));
        return channel -> client.execute(ExecuteEnrichPolicyAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
