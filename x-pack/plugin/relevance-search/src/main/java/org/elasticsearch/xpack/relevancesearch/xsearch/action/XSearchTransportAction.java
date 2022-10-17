/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.xsearch.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.relevancesearch.query.RelevanceMatchQueryBuilder;
import org.elasticsearch.xpack.relevancesearch.query.RelevanceMatchQueryRewriter;
import org.elasticsearch.xpack.relevancesearch.xsearch.XSearchRequestValidationService;

public class XSearchTransportAction extends HandledTransportAction<XSearchAction.Request, SearchResponse> {

    private final RelevanceMatchQueryRewriter relevanceMatchQueryRewriter;

    private final NodeClient client;

    private final XSearchRequestValidationService xSearchRequestValidationService;

    @Inject
    public XSearchTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        String executor,
        RelevanceMatchQueryRewriter relevanceMatchQueryRewriter,
        NodeClient client,
        XSearchRequestValidationService xSearchRequestValidationService
    ) {
        super(XSearchAction.NAME, false, transportService, actionFilters, XSearchAction.Request::new, executor);
        this.relevanceMatchQueryRewriter = relevanceMatchQueryRewriter;
        this.client = client;
        this.xSearchRequestValidationService = xSearchRequestValidationService;
    }

    @Override
    protected void doExecute(Task task, XSearchAction.Request request, ActionListener<SearchResponse> listener) {

        try {
            xSearchRequestValidationService.validateRequest(request);
        } catch (XSearchRequestValidationService.InvalidXSearchRequestException e) {
            throw new IllegalArgumentException(e);
        }

        String[] indices = request.indices();
        QueryBuilder queryBuilder = new RelevanceMatchQueryBuilder(relevanceMatchQueryRewriter, request.getQuery());
        // TODO at some point size should be part of the request
        SearchRequest searchRequest = client.prepareSearch(indices).setQuery(queryBuilder).setSize(1000).setFetchSource(true).request();

        client.search(searchRequest, listener.delegateFailure((l, searchResponse) -> {
            try {
                l.onResponse(searchResponse);
            } catch (Exception t) {
                l.onFailure(t);
            }
        }));
    }
}
