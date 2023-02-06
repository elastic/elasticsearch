/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.QueryRulesListAction;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.QUERY_RULES_MANAGEMENT_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.searchbusinessrules.SearchBusinessRules.QUERY_RULES_CONCRETE_INDEX_NAME;

public class TransportQueryRulesListAction extends HandledTransportAction<QueryRulesListAction.Request, QueryRulesListAction.Response> {

    private final Client client;

    @Inject
    public TransportQueryRulesListAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(QueryRulesListAction.NAME, transportService, actionFilters, QueryRulesListAction.Request::new);
        this.client = new OriginSettingClient(client, QUERY_RULES_MANAGEMENT_ORIGIN);
    }

    @Override
    protected void doExecute(Task task, QueryRulesListAction.Request request, ActionListener<QueryRulesListAction.Response> listener) {
        SearchRequest searchRequest = new SearchRequest(QUERY_RULES_CONCRETE_INDEX_NAME).source(
            new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                .fetchSource(false)
                .size(request.getSize())
                .from(request.getFrom())
        );

        executeAsyncWithOrigin(
            client,
            QUERY_RULES_MANAGEMENT_ORIGIN,
            SearchAction.INSTANCE,
            searchRequest,
            new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    logger.info("1 >>> " + Strings.toString(response));
                    List<String> ids = Arrays.stream(response.getHits().getHits()).map(hit -> hit.getId()).toList();

                    listener.onResponse(new QueryRulesListAction.Response(ids.toArray(new String[ids.size()])));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );
    }
}
