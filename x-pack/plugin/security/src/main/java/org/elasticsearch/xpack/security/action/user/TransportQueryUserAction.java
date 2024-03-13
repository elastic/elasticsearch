/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.ActionTypes;
import org.elasticsearch.xpack.core.security.action.user.QueryUserRequest;
import org.elasticsearch.xpack.core.security.action.user.QueryUserResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.profile.ProfileService;
import org.elasticsearch.xpack.security.support.UserBoolQueryBuilder;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.security.support.UserBoolQueryBuilder.USER_FIELD_NAME_TRANSLATOR;

public final class TransportQueryUserAction extends TransportAction<QueryUserRequest, QueryUserResponse> {
    private final NativeUsersStore usersStore;
    private final ProfileService profileService;
    private final Authentication.RealmRef nativeRealmRef;
    private static final Set<String> FIELD_NAMES_WITH_SORT_SUPPORT = Set.of("username", "roles", "enabled");

    @Inject
    public TransportQueryUserAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NativeUsersStore usersStore,
        ProfileService profileService,
        Realms realms
    ) {
        super(ActionTypes.QUERY_USER_ACTION.name(), actionFilters, transportService.getTaskManager());
        this.usersStore = usersStore;
        this.profileService = profileService;
        this.nativeRealmRef = realms.getNativeRealmRef();
    }

    @Override
    protected void doExecute(Task task, QueryUserRequest request, ActionListener<QueryUserResponse> listener) {
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

        searchSourceBuilder.query(UserBoolQueryBuilder.build(request.getQueryBuilder()));

        if (request.getFieldSortBuilders() != null) {
            translateFieldSortBuilders(request.getFieldSortBuilders(), searchSourceBuilder);
        }

        if (request.getSearchAfterBuilder() != null) {
            searchSourceBuilder.searchAfter(request.getSearchAfterBuilder().getSortValues());
        }

        final SearchRequest searchRequest = new SearchRequest(new String[] { SECURITY_MAIN_ALIAS }, searchSourceBuilder);

        usersStore.queryUsers(searchRequest, ActionListener.wrap(queryUserResults -> {
            if (request.isWithProfileUid()) {
                resolveProfileUids(queryUserResults, listener);
            } else {
                List<QueryUserResponse.Item> queryUserResponseResults = queryUserResults.userQueryResult()
                    .stream()
                    .map(queryUserResult -> new QueryUserResponse.Item(queryUserResult.user(), queryUserResult.sortValues(), null))
                    .toList();
                listener.onResponse(new QueryUserResponse(queryUserResults.total(), queryUserResponseResults));
            }
        }, listener::onFailure));
    }

    private void resolveProfileUids(NativeUsersStore.QueryUserResults queryUserResults, ActionListener<QueryUserResponse> listener) {
        final List<Subject> subjects = queryUserResults.userQueryResult()
            .stream()
            .map(item -> new Subject(item.user(), nativeRealmRef))
            .toList();

        profileService.searchProfilesForSubjects(subjects, ActionListener.wrap(resultsAndErrors -> {
            if (resultsAndErrors == null || resultsAndErrors.errors().isEmpty()) {
                final Map<String, String> profileUidLookup = resultsAndErrors == null
                    ? Map.of()
                    : resultsAndErrors.results()
                        .stream()
                        .filter(t -> Objects.nonNull(t.v2()))
                        .map(t -> new Tuple<>(t.v1().getUser().principal(), t.v2().uid()))
                        .collect(Collectors.toUnmodifiableMap(Tuple::v1, Tuple::v2));

                List<QueryUserResponse.Item> queryUserResponseResults = queryUserResults.userQueryResult()
                    .stream()
                    .map(
                        userResult -> new QueryUserResponse.Item(
                            userResult.user(),
                            userResult.sortValues(),
                            profileUidLookup.getOrDefault(userResult.user().principal(), null)
                        )
                    )
                    .toList();
                listener.onResponse(new QueryUserResponse(queryUserResults.total(), queryUserResponseResults));
            } else {
                final ElasticsearchStatusException exception = new ElasticsearchStatusException(
                    "failed to retrieve profile for users. please retry without fetching profile uid (with_profile_uid=false)",
                    RestStatus.INTERNAL_SERVER_ERROR
                );
                resultsAndErrors.errors().values().forEach(exception::addSuppressed);
                listener.onFailure(exception);
            }
        }, listener::onFailure));
    }

    // package private for testing
    static void translateFieldSortBuilders(List<FieldSortBuilder> fieldSortBuilders, SearchSourceBuilder searchSourceBuilder) {
        fieldSortBuilders.forEach(fieldSortBuilder -> {
            if (fieldSortBuilder.getNestedSort() != null) {
                throw new IllegalArgumentException("nested sorting is not supported for User query");
            }
            if (FieldSortBuilder.DOC_FIELD_NAME.equals(fieldSortBuilder.getFieldName())) {
                searchSourceBuilder.sort(fieldSortBuilder);
            } else {
                final String translatedFieldName = USER_FIELD_NAME_TRANSLATOR.translate(fieldSortBuilder.getFieldName());
                if (FIELD_NAMES_WITH_SORT_SUPPORT.contains(translatedFieldName) == false) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "sorting is not supported for field [%s] in User query", fieldSortBuilder.getFieldName())
                    );
                }

                if (translatedFieldName.equals(fieldSortBuilder.getFieldName())) {
                    searchSourceBuilder.sort(fieldSortBuilder);
                } else {
                    final FieldSortBuilder translatedFieldSortBuilder = new FieldSortBuilder(translatedFieldName).order(
                        fieldSortBuilder.order()
                    )
                        .missing(fieldSortBuilder.missing())
                        .unmappedType(fieldSortBuilder.unmappedType())
                        .setFormat(fieldSortBuilder.getFormat());

                    if (fieldSortBuilder.sortMode() != null) {
                        translatedFieldSortBuilder.sortMode(fieldSortBuilder.sortMode());
                    }
                    if (fieldSortBuilder.getNumericType() != null) {
                        translatedFieldSortBuilder.setNumericType(fieldSortBuilder.getNumericType());
                    }
                    searchSourceBuilder.sort(translatedFieldSortBuilder);
                }
            }
        });
    }
}
