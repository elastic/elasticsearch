/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestMultiSearchTemplateAction extends BaseRestHandler {
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal]"
        + " Specifying types in multi search template requests is deprecated.";

    private static final Set<String> RESPONSE_PARAMS = Set.of(RestSearchAction.TYPED_KEYS_PARAM, RestSearchAction.TOTAL_HITS_AS_INT_PARAM);

    private final boolean allowExplicitIndex;

    public RestMultiSearchTemplateAction(Settings settings) {
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_msearch/template"),
            new Route(POST, "/_msearch/template"),
            new Route(GET, "/{index}/_msearch/template"),
            new Route(POST, "/{index}/_msearch/template"),
            Route.builder(GET, "/{index}/{type}/_msearch/template").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(POST, "/{index}/{type}/_msearch/template").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "multi_search_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        MultiSearchTemplateRequest multiRequest = parseRequest(request, allowExplicitIndex);
        return channel -> client.execute(MultiSearchTemplateAction.INSTANCE, multiRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Parses a {@link RestRequest} body and returns a {@link MultiSearchTemplateRequest}
     */
    public static MultiSearchTemplateRequest parseRequest(RestRequest restRequest, boolean allowExplicitIndex) throws IOException {
        if (restRequest.getRestApiVersion() == RestApiVersion.V_7 && restRequest.hasParam("type")) {
            restRequest.param("type");
        }

        MultiSearchTemplateRequest multiRequest = new MultiSearchTemplateRequest();
        if (restRequest.hasParam("max_concurrent_searches")) {
            multiRequest.maxConcurrentSearchRequests(restRequest.paramAsInt("max_concurrent_searches", 0));
        }

        RestMultiSearchAction.parseMultiLineRequest(
            restRequest,
            multiRequest.indicesOptions(),
            allowExplicitIndex,
            (searchRequest, bytes) -> {
                SearchTemplateRequest searchTemplateRequest = SearchTemplateRequest.fromXContent(bytes);
                if (searchTemplateRequest.getScript() != null) {
                    searchTemplateRequest.setRequest(searchRequest);
                    multiRequest.add(searchTemplateRequest);
                } else {
                    throw new IllegalArgumentException("Malformed search template");
                }
                RestSearchAction.validateSearchRequest(restRequest, searchRequest);
            }
        );
        return multiRequest;
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
