/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;

import java.io.IOException;

public class RestGetEnrichPolicyAction extends BaseRestHandler {

    public RestGetEnrichPolicyAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, "/_enrich/policy/{name}", this);
    }

    @Override
    public String getName() {
        return "get_enrich_policy";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) throws IOException {
        final GetEnrichPolicyAction.Request request = new GetEnrichPolicyAction.Request(restRequest.param("name"));
        return channel -> client.execute(GetEnrichPolicyAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
