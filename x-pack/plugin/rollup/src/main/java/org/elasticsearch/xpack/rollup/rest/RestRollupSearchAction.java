/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.rest;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;

import java.io.IOException;

public class RestRollupSearchAction extends BaseRestHandler {

    public RestRollupSearchAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, "_rollup_search", this);
        controller.registerHandler(RestRequest.Method.POST, "_rollup_search", this);
        controller.registerHandler(RestRequest.Method.GET, "{index}/_rollup_search", this);
        controller.registerHandler(RestRequest.Method.POST,  "{index}/_rollup_search", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        restRequest.withContentOrSourceParamParserOrNull(parser ->
                RestSearchAction.parseSearchRequest(searchRequest, restRequest, parser, size -> searchRequest.source().size(size)));
        return channel -> client.execute(RollupSearchAction.INSTANCE, searchRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "rollup_search_action";
    }
}
