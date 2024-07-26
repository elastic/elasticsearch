/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.profile.ProfileService;
import org.elasticsearch.xpack.security.support.ApiKeyAggregationsBuilder;
import org.elasticsearch.xpack.security.support.ApiKeyBoolQueryBuilder;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.security.support.FieldNameTranslators.API_KEY_FIELD_NAME_TRANSLATORS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

public final class TransportQueryApiKeyAction extends TransportAction<QueryApiKeyRequest, QueryApiKeyResponse> {

    // API keys with no "type" field are implicitly of type "rest" (this is the case for all API Keys created before v8.9).
    // The below runtime field ensures that the "type" field can be used by the {@link RestQueryApiKeyAction},
    // while making the implicit "rest" type feature transparent to the caller (hence all keys are either "rest"
    // or "cross_cluster", and the "type" is always set).
    // This can be improved, to get rid of the runtime performance impact of the runtime field, by reindexing
    // the api key docs and setting the "type" to "rest" if empty. But the infrastructure to run such a maintenance
    // task on a system index (once the cluster version permits) is not currently available.
    public static final String API_KEY_TYPE_RUNTIME_MAPPING_FIELD = "runtime_key_type";
    private static final Map<String, Object> API_KEY_TYPE_RUNTIME_MAPPING = Map.of(
        API_KEY_TYPE_RUNTIME_MAPPING_FIELD,
        Map.of("type", "keyword", "script", Map.of("source", "emit(field('type').get(\"rest\"));"))
    );

    private final ApiKeyService apiKeyService;
    private final SecurityContext securityContext;
    private final ProfileService profileService;

    @Inject
    public TransportQueryApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ApiKeyService apiKeyService,
        SecurityContext context,
        ProfileService profileService
    ) {
        super(QueryApiKeyAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.apiKeyService = apiKeyService;
        this.securityContext = context;
        this.profileService = profileService;
    }

    @Override
    protected void doExecute(Task task, QueryApiKeyRequest request, ActionListener<QueryApiKeyResponse> listener) {
        Authentication filteringAuthentication = securityContext.getAuthentication();
        if (filteringAuthentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
        }
        if (request.isFilterForCurrentUser() == false) {
            filteringAuthentication = null;
        }

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

        final AtomicBoolean accessesApiKeyTypeField = new AtomicBoolean(false);
        searchSourceBuilder.query(ApiKeyBoolQueryBuilder.build(request.getQueryBuilder(), fieldName -> {
            if (API_KEY_TYPE_RUNTIME_MAPPING_FIELD.equals(fieldName)) {
                accessesApiKeyTypeField.set(true);
            }
        }, filteringAuthentication));

        if (request.getFieldSortBuilders() != null) {
            API_KEY_FIELD_NAME_TRANSLATORS.translateFieldSortBuilders(request.getFieldSortBuilders(), searchSourceBuilder, fieldName -> {
                if (API_KEY_TYPE_RUNTIME_MAPPING_FIELD.equals(fieldName)) {
                    accessesApiKeyTypeField.set(true);
                }
            });
        }

        searchSourceBuilder.aggregationsBuilder(ApiKeyAggregationsBuilder.process(request.getAggsBuilder(), fieldName -> {
            if (API_KEY_TYPE_RUNTIME_MAPPING_FIELD.equals(fieldName)) {
                accessesApiKeyTypeField.set(true);
            }
        }));

        // only add the query-level runtime field to the search request if it's actually referring the "type" field
        if (accessesApiKeyTypeField.get()) {
            searchSourceBuilder.runtimeMappings(API_KEY_TYPE_RUNTIME_MAPPING);
        }

        if (request.getSearchAfterBuilder() != null) {
            searchSourceBuilder.searchAfter(request.getSearchAfterBuilder().getSortValues());
        }

        final SearchRequest searchRequest = new SearchRequest(new String[] { SECURITY_MAIN_ALIAS }, searchSourceBuilder);
        apiKeyService.queryApiKeys(searchRequest, request.withLimitedBy(), ActionListener.wrap(queryApiKeysResult -> {
            if (request.withProfileUid()) {
                profileService.resolveProfileUidsForApiKeys(
                    queryApiKeysResult.apiKeyInfos(),
                    ActionListener.wrap(
                        ownerProfileUids -> listener.onResponse(
                            new QueryApiKeyResponse(
                                queryApiKeysResult.total(),
                                queryApiKeysResult.apiKeyInfos(),
                                queryApiKeysResult.sortValues(),
                                ownerProfileUids,
                                queryApiKeysResult.aggregations()
                            )
                        ),
                        listener::onFailure
                    )
                );
            } else {
                listener.onResponse(
                    new QueryApiKeyResponse(
                        queryApiKeysResult.total(),
                        queryApiKeysResult.apiKeyInfos(),
                        queryApiKeysResult.sortValues(),
                        null,
                        queryApiKeysResult.aggregations()
                    )
                );
            }
        }, listener::onFailure));
    }
}
