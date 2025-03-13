/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.role;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.ActionTypes;
import org.elasticsearch.xpack.core.security.action.role.QueryRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.QueryRoleResponse;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.support.FieldNameTranslators;
import org.elasticsearch.xpack.security.support.RoleBoolQueryBuilder;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.security.support.FieldNameTranslators.ROLE_FIELD_NAME_TRANSLATORS;

public class TransportQueryRoleAction extends TransportAction<QueryRoleRequest, QueryRoleResponse> {

    public static final String ROLE_NAME_RUNTIME_MAPPING_FIELD = "runtime_role_name";
    private static final Map<String, Object> ROLE_NAME_RUNTIME_MAPPING = Map.of(
        ROLE_NAME_RUNTIME_MAPPING_FIELD,
        Map.of("type", "keyword", "script", Map.of("source", "emit(params._fields['_id'].value.substring(5));"))
    );

    private final NativeRolesStore nativeRolesStore;

    @Inject
    public TransportQueryRoleAction(ActionFilters actionFilters, NativeRolesStore nativeRolesStore, TransportService transportService) {
        super(ActionTypes.QUERY_ROLE_ACTION.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.nativeRolesStore = nativeRolesStore;
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
        if (request.getSearchAfterBuilder() != null) {
            searchSourceBuilder.searchAfter(request.getSearchAfterBuilder().getSortValues());
        }
        AtomicBoolean accessesMetadata = new AtomicBoolean(false);
        AtomicBoolean accessesRoleName = new AtomicBoolean(false);
        searchSourceBuilder.query(RoleBoolQueryBuilder.build(request.getQueryBuilder(), indexFieldName -> {
            if (indexFieldName.startsWith(FieldNameTranslators.FLATTENED_METADATA_INDEX_FIELD_NAME)) {
                accessesMetadata.set(true);
            } else if (indexFieldName.equals(ROLE_NAME_RUNTIME_MAPPING_FIELD)) {
                accessesRoleName.set(true);
            }
        }));
        if (request.getFieldSortBuilders() != null) {
            ROLE_FIELD_NAME_TRANSLATORS.translateFieldSortBuilders(request.getFieldSortBuilders(), searchSourceBuilder, indexFieldName -> {
                if (indexFieldName.startsWith(FieldNameTranslators.FLATTENED_METADATA_INDEX_FIELD_NAME)) {
                    accessesMetadata.set(true);
                } else if (indexFieldName.equals(ROLE_NAME_RUNTIME_MAPPING_FIELD)) {
                    accessesRoleName.set(true);
                }
            });
        }
        if (accessesMetadata.get() && nativeRolesStore.isMetadataSearchable() == false) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot query or sort role metadata until automatic migration completed",
                    RestStatus.SERVICE_UNAVAILABLE
                )
            );
            return;
        }
        // only add the query-level runtime field to the search request if it's actually referring the role name
        if (accessesRoleName.get()) {
            searchSourceBuilder.runtimeMappings(ROLE_NAME_RUNTIME_MAPPING);
        }
        nativeRolesStore.queryRoleDescriptors(
            searchSourceBuilder,
            ActionListener.wrap(
                queryRoleResults -> listener.onResponse(new QueryRoleResponse(queryRoleResults.total(), queryRoleResults.items())),
                listener::onFailure
            )
        );
    }
}
