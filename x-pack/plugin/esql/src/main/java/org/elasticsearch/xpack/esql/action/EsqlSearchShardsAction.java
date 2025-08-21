/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * A fork of the search shards API for ES|QL. This fork allows us to gradually introduce features and optimizations to this
 * internal API without risking breaking the search-shards API, which is used by the search API. For now, this API delegates
 * to the search-shards API, but gradually, we will decouple this API completely from the search-shards API.
 */
public class EsqlSearchShardsAction extends HandledTransportAction<SearchShardsRequest, SearchShardsResponse> {
    public static final String NAME = "indices:data/read/esql/search_shards";
    public static final ActionType<SearchShardsResponse> TYPE = new ActionType<>(NAME);
    private final TransportSearchShardsAction searchShardsAction;

    @Inject
    public EsqlSearchShardsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        TransportSearchShardsAction searchShardsAction
    ) {
        super(
            NAME,
            transportService,
            actionFilters,
            SearchShardsRequest::new,
            // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH_COORDINATION)
        );
        this.searchShardsAction = searchShardsAction;
    }

    @Override
    protected void doExecute(Task task, SearchShardsRequest request, ActionListener<SearchShardsResponse> listener) {
        searchShardsAction.searchShards(task, request, listener);
    }
}
