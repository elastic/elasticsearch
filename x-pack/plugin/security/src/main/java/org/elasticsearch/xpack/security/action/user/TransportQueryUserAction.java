/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.ActionTypes;
import org.elasticsearch.xpack.core.security.action.user.QueryUserRequest;
import org.elasticsearch.xpack.core.security.action.user.QueryUserResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.profile.ProfileService;
import org.elasticsearch.xpack.security.support.UserBoolQueryBuilder;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.support.FieldNameTranslators.USER_FIELD_NAME_TRANSLATORS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

public final class TransportQueryUserAction extends TransportAction<QueryUserRequest, QueryUserResponse> {
    private final NativeUsersStore usersStore;
    private final ProfileService profileService;
    private final Authentication.RealmRef nativeRealmRef;

    @Inject
    public TransportQueryUserAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NativeUsersStore usersStore,
        ProfileService profileService,
        Realms realms
    ) {
        super(ActionTypes.QUERY_USER_ACTION.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
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
            USER_FIELD_NAME_TRANSLATORS.translateFieldSortBuilders(request.getFieldSortBuilders(), searchSourceBuilder, null);
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
}
