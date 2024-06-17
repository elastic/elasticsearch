/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.role;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.role.QueryRoleAction;
import org.elasticsearch.xpack.core.security.action.role.QueryRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.QueryRoleResponse;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.support.FieldNameTranslators;
import org.elasticsearch.xpack.security.support.RoleBoolQueryBuilder;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.security.support.FieldNameTranslators.ROLE_FIELD_NAME_TRANSLATORS;
import static org.elasticsearch.xpack.security.support.SecurityMigrations.ROLE_METADATA_FLATTENED_MIGRATION_VERSION;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

public class TransportQueryRoleAction extends TransportAction<QueryRoleRequest, QueryRoleResponse> {

    private final NativeRolesStore nativeRolesStore;
    private final SecurityIndexManager securityIndex;

    @Inject
    public TransportQueryRoleAction(
        ActionFilters actionFilters,
        NativeRolesStore nativeRolesStore,
        SecurityIndexManager securityIndex,
        TransportService transportService
    ) {
        super(QueryRoleAction.NAME, actionFilters, transportService.getTaskManager());
        this.nativeRolesStore = nativeRolesStore;
        this.securityIndex = securityIndex;
    }

    @Override
    protected void doExecute(Task task, QueryRoleRequest request, ActionListener<QueryRoleResponse> listener) {
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().version(false).fetchSource(true).trackTotalHits(true);
        if (request.getFrom() != null) {
            searchSourceBuilder.from(request.getFrom());
        }
        if (request.getSize() != null) {
            searchSourceBuilder.size(request.getSize());
        }
        AtomicBoolean isQueryingMetadata = new AtomicBoolean(false);
        searchSourceBuilder.query(RoleBoolQueryBuilder.build(request.getQueryBuilder(), indexFieldName -> {
            if (indexFieldName.startsWith(FieldNameTranslators.FLATTENED_METADATA_INDEX_FIELD_NAME)) {
                isQueryingMetadata.set(true);
            }
        }));
        if (isQueryingMetadata.get()) {
            if (securityIndex.isMigrationsVersionAtLeast(ROLE_METADATA_FLATTENED_MIGRATION_VERSION) == false) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Cannot query role metadata until automatic migration completed",
                        RestStatus.SERVICE_UNAVAILABLE
                    )
                );
                return;
            }
        }
        if (request.getFieldSortBuilders() != null) {
            ROLE_FIELD_NAME_TRANSLATORS.translateFieldSortBuilders(request.getFieldSortBuilders(), searchSourceBuilder, null);
        }
        if (request.getSearchAfterBuilder() != null) {
            searchSourceBuilder.searchAfter(request.getSearchAfterBuilder().getSortValues());
        }
        SearchRequest searchRequest = new SearchRequest(new String[] { SECURITY_MAIN_ALIAS }, searchSourceBuilder);
        nativeRolesStore.queryRoleDescriptors(
            searchRequest,
            ActionListener.wrap(
                queryRoleResults -> listener.onResponse(new QueryRoleResponse(queryRoleResults.total(), queryRoleResults.items())),
                listener::onFailure
            )
        );
    }
}
