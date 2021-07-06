/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.SearchApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.SearchApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.SearchApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.support.ApiKeyBoolQueryBuilder;

public final class TransportSearchApiKeyAction extends HandledTransportAction<SearchApiKeyRequest, SearchApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final SecurityContext securityContext;

    @Inject
    public TransportSearchApiKeyAction(TransportService transportService, ActionFilters actionFilters, ApiKeyService apiKeyService,
                                       SecurityContext context) {
        super(SearchApiKeyAction.NAME, transportService, actionFilters, SearchApiKeyRequest::new);
        this.apiKeyService = apiKeyService;
        this.securityContext = context;
    }

    @Override
    protected void doExecute(Task task, SearchApiKeyRequest request, ActionListener<SearchApiKeyResponse> listener) {
        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
        }

        final ApiKeyBoolQueryBuilder apiKeyBoolQueryBuilder =
            ApiKeyBoolQueryBuilder.build(request.getQueryBuilder(), request.isFilterForCurrentUser() ? authentication : null);

        apiKeyService.searchApiKeys(apiKeyBoolQueryBuilder, listener);
    }

}
