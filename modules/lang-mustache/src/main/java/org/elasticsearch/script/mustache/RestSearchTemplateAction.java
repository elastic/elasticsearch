/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSearchTemplateAction extends BaseRestHandler {

    public static final String TYPED_KEYS_PARAM = "typed_keys";
    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>(Arrays.asList(TYPED_KEYS_PARAM, RestSearchAction.TOTAL_HITS_AS_INT_PARAM));
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_search/template"),
            new Route(POST, "/_search/template"),
            new Route(GET, "/{index}/_search/template"),
            new Route(POST, "/{index}/_search/template"),
            Route.builder(GET, "/{index}/{type}/_search/template")
                .deprecated(RestSearchAction.TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build(),
            Route.builder(POST, "/{index}/{type}/_search/template")
                .deprecated(RestSearchAction.TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build()
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
            client.getNamedWriteableRegistry(),
            size -> searchRequest.source().size(size)
        );

        // Creates the search template request
        SearchTemplateRequest searchTemplateRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            searchTemplateRequest = SearchTemplateRequest.fromXContent(parser);
        }
        searchTemplateRequest.setRequest(searchRequest);

        return channel -> client.execute(SearchTemplateAction.INSTANCE, searchTemplateRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
