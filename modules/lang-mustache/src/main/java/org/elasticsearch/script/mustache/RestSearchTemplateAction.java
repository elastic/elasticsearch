/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestSearchTemplateAction extends BaseRestHandler {

    public static final String TYPED_KEYS_PARAM = "typed_keys";

    private static final Set<String> RESPONSE_PARAMS = Set.of(TYPED_KEYS_PARAM, RestSearchAction.TOTAL_HITS_AS_INT_PARAM);

    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestSearchTemplateAction(Predicate<NodeFeature> clusterSupportsFeature) {
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_search/template"),
            new Route(POST, "/_search/template"),
            new Route(GET, "/{index}/_search/template"),
            new Route(POST, "/{index}/_search/template")
        );
    }

    @Override
    public String getName() {
        return "search_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Creates the search request with all required params
        SearchRequest searchRequest = new SearchRequest();
        RestSearchAction.parseSearchRequest(
            searchRequest,
            request,
            null,
            clusterSupportsFeature,
            size -> searchRequest.source().size(size)
        );

        // Creates the search template request
        SearchTemplateRequest searchTemplateRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            searchTemplateRequest = SearchTemplateRequest.fromXContent(parser);
        }
        searchTemplateRequest.setRequest(searchRequest);
        // This param is parsed within the search request
        if (searchRequest.source().explain() != null) {
            searchTemplateRequest.setExplain(searchRequest.source().explain());
        }
        return channel -> client.execute(
            MustachePlugin.SEARCH_TEMPLATE_ACTION,
            searchTemplateRequest,
            new RestToXContentListener<>(channel, SearchTemplateResponse::status)
        );
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
