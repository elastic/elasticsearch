/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.xsearch.action.rest;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.relevancesearch.xsearch.action.XSearchSearchAction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

// TODO Suggestion from Aurelian: consider naming this GET because it isn't updating data, even though we do support POST
public class RestXSearchSearchAction extends BaseRestHandler {

    public static final String REST_BASE_PATH = "/{index}/_xsearch";

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, REST_BASE_PATH), new Route(POST, REST_BASE_PATH));
    }

    @Override
    public String getName() {
        return "xsearch_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String index = request.param("index");
        XContentParser parser = request.contentOrSourceParamParser();
        XSearchSearchAction.Request xsearchRequest = XSearchSearchAction.Request.parseRequest(index, parser);
        // Do the xsearch request
        xsearchRequest.indicesOptions(IndicesOptions.fromRequest(request, xsearchRequest.indicesOptions()));
        return channel -> client.execute(XSearchSearchAction.INSTANCE, xsearchRequest, new RestToXContentListener<>(channel));
    }

    private static final Set<String> RESPONSE_PARAMS = Collections.emptySet();

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
