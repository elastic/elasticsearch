/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.enterprisesearch.search.EntSearchQueryBuilder;

public class TransportEntSearchAction extends HandledTransportAction<EntSearchRequest, EntSearchResponse> {

    private final NodeClient client;

    @Inject
    public TransportEntSearchAction(TransportService transportService, ActionFilters actionFilters, NodeClient client) {
        super(EntSearchAction.NAME, transportService, actionFilters, EntSearchRequest::new);
        this.client = client;
    }

    private static void performQuery(EntSearchRequest request, NodeClient client, EntSearchResponse entSearchResponse, ActionListener<EntSearchResponse> listener) {
        final QueryBuilder queryBuilder = EntSearchQueryBuilder.getQueryBuilder(request);

        SearchRequest searchRequest = client.prepareSearch(request.getIndex())
            .setQuery(queryBuilder)
            .setSize(1000)
            .setFetchSource(true)
            .request();

        client.search(searchRequest, listener.delegateFailure((l, searchResponse) -> {
            try {
                entSearchResponse.setSearchResponse(searchResponse);
                l.onResponse(entSearchResponse);
            } catch (Exception t) {
                l.onFailure(t);
            }
        }));
    }

    @Override
    protected void doExecute(Task task, EntSearchRequest request, ActionListener<EntSearchResponse> listener) {

        final EntSearchResponse response = new EntSearchResponse();

        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(request.getIndex());

        client
            .admin()
            .indices()
            .getMappings(getMappingsRequest, listener.delegateFailure((l, mappingResponse) -> {
                try {
                    request.setFieldsFromFieldMapping(mappingResponse.mappings().values());
                    performQuery(request, client, response, listener);
                } catch (Exception t) {
                    l.onFailure(t);
                }
            }));

    }
}
