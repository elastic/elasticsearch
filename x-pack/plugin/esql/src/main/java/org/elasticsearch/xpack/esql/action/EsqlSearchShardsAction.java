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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class EsqlSearchShardsAction extends HandledTransportAction<SearchShardsRequest, SearchShardsResponse> {
    private static final String NAME = "indices:data/read/esql/search_shards";
    public static final ActionType<SearchShardsResponse> TYPE = new ActionType<>(NAME);

    private final TransportSearchShardsAction innerAction;

    @Inject
    public EsqlSearchShardsAction(TransportService transportService, ActionFilters actionFilters, TransportSearchShardsAction innerAction) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(NAME, transportService, actionFilters, SearchShardsRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.innerAction = innerAction;
    }

    @Override
    protected void doExecute(Task task, SearchShardsRequest request, final ActionListener<SearchShardsResponse> listener) {
        innerAction.doExecute(task, request, listener);
    }
}
