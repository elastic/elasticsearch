/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetEnrichPolicyAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_enrich/policy/{name}"), new Route(GET, "/_enrich/policy")));
    }

    @Override
    public String getName() {
        return "get_enrich_policy";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) throws IOException {
        String[] names = Strings.splitStringByCommaToArray(restRequest.param("name"));
        final GetEnrichPolicyAction.Request request = new GetEnrichPolicyAction.Request(names);
        return channel -> client.execute(GetEnrichPolicyAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
