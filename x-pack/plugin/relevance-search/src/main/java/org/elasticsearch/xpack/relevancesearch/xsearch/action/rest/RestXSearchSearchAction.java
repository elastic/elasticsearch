/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.xsearch.action.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.relevancesearch.xsearch.action.XSearchSearchAction;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestXSearchSearchAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_xsearch"), new Route(POST, "/{index}/_xsearch"));
    }

    @Override
    public String getName() {
        return "xsearch";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest restRequest, final NodeClient client) {
        String engine = Strings.splitStringByCommaToArray(restRequest.param("engine"))[0];
        return channel -> client.execute(
            XSearchSearchAction.INSTANCE,
            new XSearchSearchAction.Request(),
            new RestToXContentListener<>(channel)
        );
    }

    private static final Set<String> RESPONSE_PARAMS = Collections.emptySet();

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
