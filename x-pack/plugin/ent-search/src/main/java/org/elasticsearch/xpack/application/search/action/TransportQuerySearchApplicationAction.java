/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.application.search.SearchApplicationIndexService;
import org.elasticsearch.xpack.application.search.SearchApplicationTemplateService;

import java.util.Map;

public class TransportQuerySearchApplicationAction extends HandledTransportAction<SearchApplicationSearchRequest, SearchResponse> {

    private static final Logger logger = LogManager.getLogger(TransportQuerySearchApplicationAction.class);
    protected final SearchApplicationIndexService systemIndexService;
    private final Client client;

    private final SearchApplicationTemplateService templateService;

    @Inject
    public TransportQuerySearchApplicationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        NamedWriteableRegistry namedWriteableRegistry,
        BigArrays bigArrays,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry
    ) {
        super(QuerySearchApplicationAction.NAME, transportService, actionFilters, SearchApplicationSearchRequest::new);
        this.client = client;
        this.templateService = new SearchApplicationTemplateService(scriptService, xContentRegistry);
        this.systemIndexService = new SearchApplicationIndexService(client, clusterService, namedWriteableRegistry, bigArrays);
    }

    @Override
    protected void doExecute(Task task, SearchApplicationSearchRequest request, ActionListener<SearchResponse> listener) {
        systemIndexService.getSearchApplication(request.name(), listener.delegateFailure((l, searchApplication) -> {
            try {
                SearchSourceBuilder sourceBuilder = templateService.renderQuery(searchApplication, request.queryParams());
                SearchRequest searchRequest = new SearchRequest(searchApplication.name()).source(sourceBuilder);

                systemIndexService.checkAliasConsistency(searchApplication, new ActionListener<>() {
                    @Override
                    public void onResponse(Map<String, String> stringStringMap) {
                        client.execute(
                            SearchAction.INSTANCE,
                            searchRequest,
                            listener.delegateFailure((l2, searchResponse) -> l2.onResponse(new SearchApplicationSearchResponse(searchResponse, stringStringMap)))
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            } catch (Exception e) {
                l.onFailure(e);
            }
        }));
    }
}
