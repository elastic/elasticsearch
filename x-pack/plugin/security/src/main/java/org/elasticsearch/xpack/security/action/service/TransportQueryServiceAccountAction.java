/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountResponse;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.support.ServiceAccountBoolQueryBuilder;

import static org.elasticsearch.xpack.security.support.FieldNameTranslators.SERVICE_ACCOUNT_FIELD_NAME_TRANSLATORS;

/**
 * Turns a query request into a search of the security index. The caller's query and sort are translated and
 * restricted here, before the search is built, so that the store only ever sees a query that cannot reach past the
 * service-account documents.
 */
public final class TransportQueryServiceAccountAction extends TransportAction<QueryServiceAccountRequest, QueryServiceAccountResponse> {

    private final ServiceAccountService serviceAccountService;

    @Inject
    public TransportQueryServiceAccountAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServiceAccountService serviceAccountService
    ) {
        super(QueryServiceAccountAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.serviceAccountService = serviceAccountService;
    }

    @Override
    protected void doExecute(Task task, QueryServiceAccountRequest request, ActionListener<QueryServiceAccountResponse> listener) {
        final SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
            .version(false)
            .fetchSource(true)
            .trackTotalHits(true);
        if (request.getFrom() != null) {
            searchSourceBuilder.from(request.getFrom());
        }
        if (request.getSize() != null) {
            searchSourceBuilder.size(request.getSize());
        }
        searchSourceBuilder.query(ServiceAccountBoolQueryBuilder.build(request.getQueryBuilder()));
        if (request.getFieldSortBuilders() != null) {
            SERVICE_ACCOUNT_FIELD_NAME_TRANSLATORS.translateFieldSortBuilders(request.getFieldSortBuilders(), searchSourceBuilder, null);
        }
        if (request.getSearchAfterBuilder() != null) {
            searchSourceBuilder.searchAfter(request.getSearchAfterBuilder().getSortValues());
        }
        serviceAccountService.queryUserManagedAccounts(searchSourceBuilder, listener);
    }
}
