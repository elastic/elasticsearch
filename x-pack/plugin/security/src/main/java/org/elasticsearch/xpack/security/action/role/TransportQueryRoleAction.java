/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.role;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.role.QueryRoleAction;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.support.ApiKeyBoolQueryBuilder;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.security.support.ApiKeyFieldNameTranslators.translateFieldSortBuilders;

public class TransportQueryRoleAction extends TransportAction<QueryApiKeyRequest, QueryApiKeyResponse> {

    private final NativeRolesStore nativeRolesStore;

    @Inject
    public TransportQueryRoleAction(ActionFilters actionFilters, NativeRolesStore nativeRolesStore, TransportService transportService) {
        super(QueryRoleAction.NAME, actionFilters, transportService.getTaskManager());
        this.nativeRolesStore = nativeRolesStore;
    }

    @Override
    protected void doExecute(Task task, QueryApiKeyRequest request, ActionListener<QueryApiKeyResponse> listener) {
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
        searchSourceBuilder.query(ApiKeyBoolQueryBuilder.build(request.getQueryBuilder(), fieldName -> {
            if (API_KEY_TYPE_RUNTIME_MAPPING_FIELD.equals(fieldName)) {
                accessesApiKeyTypeField.set(true);
            }
        }, filteringAuthentication));

        if (request.getFieldSortBuilders() != null) {
            translateFieldSortBuilders(request.getFieldSortBuilders(), searchSourceBuilder, fieldName -> {
                if (API_KEY_TYPE_RUNTIME_MAPPING_FIELD.equals(fieldName)) {
                    accessesApiKeyTypeField.set(true);
                }
            });
        }
    }
}
