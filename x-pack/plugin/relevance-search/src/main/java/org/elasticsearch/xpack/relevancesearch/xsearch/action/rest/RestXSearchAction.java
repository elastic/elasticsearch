/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.xsearch.action.rest;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.relevancesearch.query.RelevanceMatchQueryBuilder;
import org.elasticsearch.xpack.relevancesearch.query.RelevanceMatchQueryRewriter;
import org.elasticsearch.xpack.relevancesearch.xsearch.XSearchRequestValidationService;
import org.elasticsearch.xpack.relevancesearch.xsearch.action.XSearchAction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestXSearchAction extends BaseRestHandler {

    public static final String REST_BASE_PATH = "/{index}/_xsearch";

    private final RelevanceMatchQueryRewriter relevanceMatchQueryRewriter;
    private final XSearchRequestValidationService xSearchRequestValidationService;

    @Inject
    public RestXSearchAction(
        RelevanceMatchQueryRewriter relevanceMatchQueryRewriter,
        XSearchRequestValidationService xSearchRequestValidationService
    ) {
        super();
        this.relevanceMatchQueryRewriter = relevanceMatchQueryRewriter;
        this.xSearchRequestValidationService = xSearchRequestValidationService;
    }

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
        boolean explain = request.paramAsBoolean("explain", false);
        XContentParser parser = request.contentOrSourceParamParser();
        XSearchAction.Request xsearchRequest = XSearchAction.Request.parseRequest(index, parser, explain);
        xsearchRequest.indicesOptions(IndicesOptions.fromRequest(request, xsearchRequest.indicesOptions()));

        xSearchRequestValidationService.validateRequest(xsearchRequest);

        RelevanceMatchQueryBuilder queryBuilder = new RelevanceMatchQueryBuilder(relevanceMatchQueryRewriter, xsearchRequest.getQuery());
        return channel -> doXSearch(index, xsearchRequest.explain(), queryBuilder, client, channel);
    }

    private static void doXSearch(
        String index,
        boolean explain,
        RelevanceMatchQueryBuilder queryBuilder,
        NodeClient client,
        RestChannel channel
    ) {
        SearchRequest searchRequest = client.prepareSearch(index)
            .setQuery(queryBuilder)
            .setSize(1000)
            .setFetchSource(true)
            .setExplain(explain)
            .request();

        client.execute(XSearchAction.INSTANCE, searchRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.emptySet();
    }
}
