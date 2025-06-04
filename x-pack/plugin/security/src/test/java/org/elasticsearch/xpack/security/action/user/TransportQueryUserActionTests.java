/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.user.QueryUserRequest;
import org.elasticsearch.xpack.core.security.action.user.QueryUserResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.profile.ProfileService;
import org.mockito.ArgumentMatchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.security.support.FieldNameTranslators.USER_FIELD_NAME_TRANSLATORS;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportQueryUserActionTests extends ESTestCase {
    private static final String[] allowedIndexFieldNames = new String[] { "username", "roles", "enabled" };

    public void testTranslateFieldSortBuilders() {
        final List<String> fieldNames = List.of(allowedIndexFieldNames);

        final List<FieldSortBuilder> originals = fieldNames.stream().map(this::randomFieldSortBuilderWithName).toList();

        final SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        USER_FIELD_NAME_TRANSLATORS.translateFieldSortBuilders(originals, searchSourceBuilder, null);

        IntStream.range(0, originals.size()).forEach(i -> {
            final FieldSortBuilder original = originals.get(i);
            final FieldSortBuilder translated = (FieldSortBuilder) searchSourceBuilder.sorts().get(i);
            assertThat(original.getFieldName(), equalTo(translated.getFieldName()));

            assertThat(translated.order(), equalTo(original.order()));
            assertThat(translated.missing(), equalTo(original.missing()));
            assertThat(translated.unmappedType(), equalTo(original.unmappedType()));
            assertThat(translated.getNumericType(), equalTo(original.getNumericType()));
            assertThat(translated.getFormat(), equalTo(original.getFormat()));
            assertThat(translated.sortMode(), equalTo(original.sortMode()));
        });
    }

    public void testNestedSortingIsNotAllowed() {
        final FieldSortBuilder fieldSortBuilder = new FieldSortBuilder("roles");
        fieldSortBuilder.setNestedSort(new NestedSortBuilder("something"));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> USER_FIELD_NAME_TRANSLATORS.translateFieldSortBuilders(
                List.of(fieldSortBuilder),
                SearchSourceBuilder.searchSource(),
                null
            )
        );
        assertThat(e.getMessage(), equalTo("nested sorting is not currently supported in this context"));
    }

    public void testNestedSortingOnTextFieldsNotAllowed() {
        String fieldName = randomFrom("full_name", "email");
        final List<String> fieldNames = List.of(fieldName);
        final List<FieldSortBuilder> originals = fieldNames.stream().map(this::randomFieldSortBuilderWithName).toList();
        final SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> USER_FIELD_NAME_TRANSLATORS.translateFieldSortBuilders(originals, searchSourceBuilder, null)
        );
        assertThat(e.getMessage(), equalTo(Strings.format("sorting is not supported for field [%s]", fieldName)));
    }

    public void testQueryUsers() {
        final List<User> storeUsers = randomFrom(
            Collections.singletonList(new User("joe")),
            Arrays.asList(new User("jane"), new User("fred")),
            randomUsers()
        );
        final boolean profileIndexExists = randomBoolean();
        NativeUsersStore usersStore = mock(NativeUsersStore.class);

        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );

        TransportQueryUserAction action = new TransportQueryUserAction(
            transportService,
            mock(ActionFilters.class),
            usersStore,
            mockProfileService(false, profileIndexExists),
            mockRealms()
        );
        boolean withProfileUid = randomBoolean();
        QueryUserRequest request = new QueryUserRequest(null, null, null, null, null, withProfileUid);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            @SuppressWarnings("unchecked")
            ActionListener<NativeUsersStore.QueryUserResults> listener = (ActionListener<NativeUsersStore.QueryUserResults>) args[1];

            listener.onResponse(
                new NativeUsersStore.QueryUserResults(
                    storeUsers.stream().map(user -> new NativeUsersStore.QueryUserResult(user, null)).toList(),
                    storeUsers.size()
                )
            );
            return null;
        }).when(usersStore).queryUsers(ArgumentMatchers.any(SearchRequest.class), anyActionListener());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<QueryUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(QueryUserResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        assertEquals(responseRef.get().getItems().length, storeUsers.size());

        if (profileIndexExists && withProfileUid) {
            assertEquals(
                storeUsers.stream().map(user -> "u_profile_" + user.principal()).toList(),
                Arrays.stream(responseRef.get().getItems()).map(QueryUserResponse.Item::profileUid).toList()
            );
        } else {
            for (QueryUserResponse.Item item : responseRef.get().getItems()) {
                assertThat(item.profileUid(), nullValue());
            }
        }
    }

    public void testQueryUsersWithProfileUidException() {
        final List<User> storeUsers = randomFrom(
            Collections.singletonList(new User("joe")),
            Arrays.asList(new User("jane"), new User("fred")),
            randomUsers()
        );
        NativeUsersStore usersStore = mock(NativeUsersStore.class);

        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );

        TransportQueryUserAction action = new TransportQueryUserAction(
            transportService,
            mock(ActionFilters.class),
            usersStore,
            mockProfileService(true, true),
            mockRealms()
        );

        QueryUserRequest request = new QueryUserRequest(null, null, null, null, null, true);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            @SuppressWarnings("unchecked")
            ActionListener<NativeUsersStore.QueryUserResults> listener = (ActionListener<NativeUsersStore.QueryUserResults>) args[1];

            listener.onResponse(
                new NativeUsersStore.QueryUserResults(
                    storeUsers.stream().map(user -> new NativeUsersStore.QueryUserResult(user, null)).toList(),
                    storeUsers.size()
                )
            );
            return null;
        }).when(usersStore).queryUsers(ArgumentMatchers.any(SearchRequest.class), anyActionListener());

        final PlainActionFuture<QueryUserResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);

        final ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, future::actionGet);

        assertThat(e.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(e.getSuppressed().length, greaterThan(0));
        Arrays.stream(e.getSuppressed()).forEach(suppressed -> {
            assertThat(suppressed, instanceOf(ElasticsearchException.class));
            assertThat(suppressed.getMessage(), equalTo("something is not right"));
        });
    }

    private List<User> randomUsers() {
        int size = scaledRandomIntBetween(3, 16);
        List<User> users = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            users.add(new User("user_" + i, randomAlphaOfLengthBetween(4, 12)));
        }
        return users;
    }

    private Profile profileFromSubject(Subject subject) {
        final User user = subject.getUser();
        final Authentication.RealmRef realmRef = subject.getRealm();
        return new Profile(
            "u_profile_" + user.principal(),
            randomBoolean(),
            randomNonNegativeLong(),
            new Profile.ProfileUser(
                user.principal(),
                Arrays.asList(user.roles()),
                realmRef.getName(),
                realmRef.getDomain() == null ? null : realmRef.getDomain().name(),
                user.email(),
                user.fullName()
            ),
            Map.of(),
            Map.of(),
            new Profile.VersionControl(randomNonNegativeLong(), randomNonNegativeLong())
        );
    }

    private ProfileService mockProfileService(boolean throwException, boolean profileIndexExists) {
        final ProfileService profileService = mock(ProfileService.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final var listener = (ActionListener<ProfileService.SubjectSearchResultsAndErrors<Profile>>) invocation.getArguments()[1];
            if (false == profileIndexExists) {
                listener.onResponse(null);
                return null;
            }
            @SuppressWarnings("unchecked")
            final List<Subject> subjects = (List<Subject>) invocation.getArguments()[0];
            List<Tuple<Subject, Profile>> results = subjects.stream()
                .map(subject -> new Tuple<>(subject, profileFromSubject(subject)))
                .toList();

            final Map<Subject, Exception> errors = new HashMap<>();
            if (throwException) {
                assertThat("random exception requires non-empty results", results, not(empty()));
                final int exceptionSize = randomIntBetween(1, results.size());
                errors.putAll(
                    results.subList(0, exceptionSize)
                        .stream()
                        .collect(Collectors.toUnmodifiableMap(Tuple::v1, t -> new ElasticsearchException("something is not right")))
                );
                results = results.subList(exceptionSize - 1, results.size());
            }

            listener.onResponse(new ProfileService.SubjectSearchResultsAndErrors<>(results, errors));
            return null;
        }).when(profileService).searchProfilesForSubjects(anyList(), anyActionListener());
        return profileService;
    }

    private Realms mockRealms() {
        final Realms realms = mock(Realms.class);
        when(realms.getNativeRealmRef()).thenReturn(
            new Authentication.RealmRef(NativeRealmSettings.DEFAULT_NAME, NativeRealmSettings.TYPE, randomAlphaOfLengthBetween(3, 8), null)
        );
        return realms;
    }

    private FieldSortBuilder randomFieldSortBuilderWithName(String name) {
        final FieldSortBuilder fieldSortBuilder = new FieldSortBuilder(name);
        fieldSortBuilder.order(randomBoolean() ? SortOrder.ASC : SortOrder.DESC);
        fieldSortBuilder.setFormat(randomBoolean() ? randomAlphaOfLengthBetween(3, 16) : null);
        if (randomBoolean()) {
            fieldSortBuilder.setNumericType(randomFrom("long", "double", "date", "date_nanos"));
        }
        if (randomBoolean()) {
            fieldSortBuilder.missing(randomAlphaOfLengthBetween(3, 8));
        }
        if (randomBoolean()) {
            fieldSortBuilder.sortMode(randomFrom(SortMode.values()));
        }
        return fieldSortBuilder;
    }
}
