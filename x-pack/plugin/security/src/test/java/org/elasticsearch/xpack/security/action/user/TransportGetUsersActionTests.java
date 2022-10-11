/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.user.GetUsersRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUsersResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealmTests;
import org.elasticsearch.xpack.security.profile.ProfileService;
import org.elasticsearch.xpack.security.profile.ProfileService.SubjectSearchResultsAndErrors;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TransportGetUsersActionTests extends ESTestCase {

    private boolean anonymousEnabled;
    private Settings settings;
    private ThreadPool threadPool;
    private boolean hasAnonymousProfile;
    private boolean hasReservedProfile;
    private boolean hasNativeProfile;

    @Before
    public void maybeEnableAnonymous() {
        anonymousEnabled = randomBoolean();
        if (anonymousEnabled) {
            settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "superuser").build();
        } else {
            settings = Settings.EMPTY;
        }
        threadPool = new TestThreadPool("TransportGetUsersActionTests");
        hasAnonymousProfile = randomBoolean();
        hasReservedProfile = randomBoolean();
        hasNativeProfile = randomBoolean();
    }

    @After
    public void terminateThreadPool() throws InterruptedException {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }

    public void testAnonymousUser() {
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.isAvailable()).thenReturn(true);
        AnonymousUser anonymousUser = new AnonymousUser(settings);
        ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), settings, usersStore, anonymousUser, threadPool);
        reservedRealm.initRealmRef(
            Map.of(
                new RealmConfig.RealmIdentifier(ReservedRealm.TYPE, ReservedRealm.NAME),
                new Authentication.RealmRef(ReservedRealm.NAME, ReservedRealm.TYPE, "node")
            )
        );
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportGetUsersAction action = new TransportGetUsersAction(
            Settings.EMPTY,
            mock(ActionFilters.class),
            usersStore,
            transportService,
            reservedRealm,
            mockRealms(),
            mockProfileService()
        );

        GetUsersRequest request = new GetUsersRequest();
        request.usernames(anonymousUser.principal());
        final boolean withProfileUid = randomBoolean();
        request.setWithProfileUid(withProfileUid);

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetUsersResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<GetUsersResponse>() {
            @Override
            public void onResponse(GetUsersResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        final User[] users = responseRef.get().users();
        if (anonymousEnabled) {
            assertThat("expected array with anonymous but got: " + Arrays.toString(users), users, arrayContaining(anonymousUser));
        } else {
            assertThat("expected an empty array but got: " + Arrays.toString(users), users, emptyArray());
        }
        if (withProfileUid) {
            assertThat(
                responseRef.get().getProfileUidLookup(),
                equalTo(
                    Arrays.stream(users)
                        .filter(user -> hasAnonymousProfile)
                        .collect(Collectors.toUnmodifiableMap(User::principal, user -> "u_profile_" + user.principal()))
                )
            );
        } else {
            assertThat(responseRef.get().getProfileUidLookup(), nullValue());
        }
        verifyNoMoreInteractions(usersStore);
    }

    public void testReservedUsersOnly() {
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.isAvailable()).thenReturn(true);

        ReservedRealmTests.mockGetAllReservedUserInfo(usersStore, Collections.emptyMap());
        ReservedRealm reservedRealm = new ReservedRealm(
            mock(Environment.class),
            settings,
            usersStore,
            new AnonymousUser(settings),
            threadPool
        );
        reservedRealm.initRealmRef(
            Map.of(
                new RealmConfig.RealmIdentifier(ReservedRealm.TYPE, ReservedRealm.NAME),
                new Authentication.RealmRef(ReservedRealm.NAME, ReservedRealm.TYPE, "node")
            )
        );
        PlainActionFuture<Collection<User>> userFuture = new PlainActionFuture<>();
        reservedRealm.users(userFuture);
        final Collection<User> allReservedUsers = userFuture.actionGet();
        final int size = randomIntBetween(1, allReservedUsers.size());
        final List<User> reservedUsers = randomSubsetOf(size, allReservedUsers);
        final List<String> names = reservedUsers.stream().map(User::principal).collect(Collectors.toList());
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportGetUsersAction action = new TransportGetUsersAction(
            Settings.EMPTY,
            mock(ActionFilters.class),
            usersStore,
            transportService,
            reservedRealm,
            mockRealms(),
            mockProfileService()
        );

        logger.error("names {}", names);
        GetUsersRequest request = new GetUsersRequest();
        request.usernames(names.toArray(new String[names.size()]));
        final boolean withProfileUid = randomBoolean();
        request.setWithProfileUid(withProfileUid);

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetUsersResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<GetUsersResponse>() {
            @Override
            public void onResponse(GetUsersResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Request failed", e);
                throwableRef.set(e);
            }
        });

        User[] users = responseRef.get().users();

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(users, arrayContaining(reservedUsers.toArray(new User[reservedUsers.size()])));
        if (withProfileUid) {
            assertThat(responseRef.get().getProfileUidLookup(), equalTo(reservedUsers.stream().filter(user -> {
                if (user instanceof AnonymousUser) {
                    return hasAnonymousProfile;
                } else {
                    return hasReservedProfile;
                }
            }).collect(Collectors.toUnmodifiableMap(User::principal, user -> "u_profile_" + user.principal()))));
        } else {
            assertThat(responseRef.get().getProfileUidLookup(), nullValue());
        }
    }

    public void testGetAllUsers() {
        final List<User> storeUsers = randomFrom(
            Collections.<User>emptyList(),
            Collections.singletonList(new User("joe")),
            Arrays.asList(new User("jane"), new User("fred")),
            randomUsers()
        );
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.isAvailable()).thenReturn(true);
        ReservedRealmTests.mockGetAllReservedUserInfo(usersStore, Collections.emptyMap());
        ReservedRealm reservedRealm = new ReservedRealm(
            mock(Environment.class),
            settings,
            usersStore,
            new AnonymousUser(settings),
            threadPool
        );
        reservedRealm.initRealmRef(
            Map.of(
                new RealmConfig.RealmIdentifier(ReservedRealm.TYPE, ReservedRealm.NAME),
                new Authentication.RealmRef(ReservedRealm.NAME, ReservedRealm.TYPE, "node")
            )
        );
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportGetUsersAction action = new TransportGetUsersAction(
            Settings.EMPTY,
            mock(ActionFilters.class),
            usersStore,
            transportService,
            reservedRealm,
            mockRealms(),
            mockProfileService()
        );

        GetUsersRequest request = new GetUsersRequest();
        final boolean withProfileUid = randomBoolean();
        request.setWithProfileUid(withProfileUid);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<List<User>> listener = (ActionListener<List<User>>) args[1];
            listener.onResponse(storeUsers);
            return null;
        }).when(usersStore).getUsers(eq(Strings.EMPTY_ARRAY), anyActionListener());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetUsersResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(GetUsersResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        final List<User> expectedList = new ArrayList<>();
        PlainActionFuture<Collection<User>> userFuture = new PlainActionFuture<>();
        reservedRealm.users(userFuture);
        final Collection<User> reservedUsers = userFuture.actionGet();
        expectedList.addAll(reservedUsers);
        expectedList.addAll(storeUsers);

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(responseRef.get().users(), arrayContaining(expectedList.toArray(new User[expectedList.size()])));
        if (withProfileUid) {
            assertThat(responseRef.get().getProfileUidLookup(), equalTo(expectedList.stream().filter(user -> {
                if (user instanceof AnonymousUser) {
                    return hasAnonymousProfile;
                } else if (reservedUsers.contains(user)) {
                    return hasReservedProfile;
                } else {
                    return hasNativeProfile;
                }
            }).collect(Collectors.toUnmodifiableMap(User::principal, user -> "u_profile_" + user.principal()))));
        } else {
            assertThat(responseRef.get().getProfileUidLookup(), nullValue());
        }
        verify(usersStore, times(1)).getUsers(aryEq(Strings.EMPTY_ARRAY), anyActionListener());
    }

    public void testGetStoreOnlyUsers() {
        testGetStoreOnlyUsers(
            randomFrom(Collections.singletonList(new User("joe")), Arrays.asList(new User("jane"), new User("fred")), randomUsers())
        );
    }

    public void testGetStoreOnlyUsersWithInternalUsername() {
        testGetStoreOnlyUsers(randomUsersWithInternalUsernames());
    }

    public void testGetUsersWithProfileUidException() {
        final List<User> storeUsers = randomFrom(
            Collections.<User>emptyList(),
            Collections.singletonList(new User("joe")),
            Arrays.asList(new User("jane"), new User("fred")),
            randomUsers()
        );
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.isAvailable()).thenReturn(true);
        ReservedRealmTests.mockGetAllReservedUserInfo(usersStore, Collections.emptyMap());
        ReservedRealm reservedRealm = new ReservedRealm(
            mock(Environment.class),
            settings,
            usersStore,
            new AnonymousUser(settings),
            threadPool
        );
        reservedRealm.initRealmRef(
            Map.of(
                new RealmConfig.RealmIdentifier(ReservedRealm.TYPE, ReservedRealm.NAME),
                new Authentication.RealmRef(ReservedRealm.NAME, ReservedRealm.TYPE, "node")
            )
        );
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportGetUsersAction action = new TransportGetUsersAction(
            Settings.EMPTY,
            mock(ActionFilters.class),
            usersStore,
            transportService,
            reservedRealm,
            mockRealms(),
            mockProfileService(true)
        );

        GetUsersRequest request = new GetUsersRequest();
        request.setWithProfileUid(true);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<List<User>> listener = (ActionListener<List<User>>) args[1];
            listener.onResponse(storeUsers);
            return null;
        }).when(usersStore).getUsers(eq(Strings.EMPTY_ARRAY), anyActionListener());

        final PlainActionFuture<GetUsersResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);

        final ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, future::actionGet);

        assertThat(e.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(e.getSuppressed().length, greaterThan(0));
        Arrays.stream(e.getSuppressed()).forEach(suppressed -> {
            assertThat(suppressed, instanceOf(ElasticsearchException.class));
            assertThat(suppressed.getMessage(), equalTo("something is not right"));
        });
    }

    private void testGetStoreOnlyUsers(List<User> storeUsers) {
        final String[] storeUsernames = storeUsers.stream().map(User::principal).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY);
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        AnonymousUser anonymousUser = new AnonymousUser(settings);
        ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), settings, usersStore, anonymousUser, threadPool);
        reservedRealm.initRealmRef(
            Map.of(
                new RealmConfig.RealmIdentifier(ReservedRealm.TYPE, ReservedRealm.NAME),
                new Authentication.RealmRef(ReservedRealm.NAME, ReservedRealm.TYPE, "node")
            )
        );
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportGetUsersAction action = new TransportGetUsersAction(
            Settings.EMPTY,
            mock(ActionFilters.class),
            usersStore,
            transportService,
            reservedRealm,
            mockRealms(),
            mockProfileService()
        );

        GetUsersRequest request = new GetUsersRequest();
        request.usernames(storeUsernames);
        final boolean withProfileUid = randomBoolean();
        request.setWithProfileUid(withProfileUid);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<List<User>> listener = (ActionListener<List<User>>) args[1];
            listener.onResponse(storeUsers);
            return null;
        }).when(usersStore).getUsers(aryEq(storeUsernames), anyActionListener());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetUsersResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<GetUsersResponse>() {
            @Override
            public void onResponse(GetUsersResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        final List<User> expectedList = new ArrayList<>();
        expectedList.addAll(storeUsers);

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(responseRef.get().users(), arrayContaining(expectedList.toArray(new User[expectedList.size()])));
        if (withProfileUid) {
            assertThat(
                responseRef.get().getProfileUidLookup(),
                equalTo(
                    expectedList.stream()
                        .filter(user -> hasNativeProfile)
                        .collect(Collectors.toUnmodifiableMap(User::principal, user -> "u_profile_" + user.principal()))
                )
            );
        } else {
            assertThat(responseRef.get().getProfileUidLookup(), nullValue());
        }
        if (storeUsers.size() > 1) {
            verify(usersStore, times(1)).getUsers(aryEq(storeUsernames), anyActionListener());
        } else {
            verify(usersStore, times(1)).getUsers(aryEq(new String[] { storeUsernames[0] }), anyActionListener());
        }
    }

    public void testException() {
        final Exception e = randomFrom(new ElasticsearchSecurityException(""), new IllegalStateException(), new ValidationException());
        final List<User> storeUsers = randomFrom(
            Collections.singletonList(new User("joe")),
            Arrays.asList(new User("jane"), new User("fred")),
            randomUsers()
        );
        final String[] storeUsernames = storeUsers.stream().map(User::principal).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY);
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
        TransportGetUsersAction action = new TransportGetUsersAction(
            Settings.EMPTY,
            mock(ActionFilters.class),
            usersStore,
            transportService,
            mock(ReservedRealm.class),
            mockRealms(),
            mockProfileService()
        );

        GetUsersRequest request = new GetUsersRequest();
        request.usernames(storeUsernames);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<List<User>> listener = (ActionListener<List<User>>) args[1];
            listener.onFailure(e);
            return null;
        }).when(usersStore).getUsers(aryEq(storeUsernames), anyActionListener());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetUsersResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<GetUsersResponse>() {
            @Override
            public void onResponse(GetUsersResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(throwableRef.get(), is(notNullValue()));
        assertThat(throwableRef.get(), is(sameInstance(e)));
        assertThat(responseRef.get(), is(nullValue()));
        verify(usersStore, times(1)).getUsers(aryEq(storeUsernames), anyActionListener());
    }

    private List<User> randomUsers() {
        int size = scaledRandomIntBetween(3, 16);
        List<User> users = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            users.add(new User("user_" + i, randomAlphaOfLengthBetween(4, 12)));
        }
        return users;
    }

    private List<User> randomUsersWithInternalUsernames() {
        return AuthenticationTestHelper.randomInternalUsernames().stream().map(User::new).collect(Collectors.toList());
    }

    private Realms mockRealms() {
        final Realms realms = mock(Realms.class);
        when(realms.getRealmRefs()).thenReturn(
            Map.of(
                new RealmConfig.RealmIdentifier(NativeRealmSettings.TYPE, NativeRealmSettings.DEFAULT_NAME),
                new Authentication.RealmRef(
                    NativeRealmSettings.DEFAULT_NAME,
                    NativeRealmSettings.TYPE,
                    randomAlphaOfLengthBetween(3, 8),
                    null
                )
            )
        );
        return realms;
    }

    private ProfileService mockProfileService() {
        return mockProfileService(false);
    }

    @SuppressWarnings("unchecked")
    private ProfileService mockProfileService(boolean randomException) {
        final ProfileService profileService = mock(ProfileService.class);
        doAnswer(invocation -> {
            final List<Subject> subjects = (List<Subject>) invocation.getArguments()[0];
            final var listener = (ActionListener<SubjectSearchResultsAndErrors<Profile>>) invocation.getArguments()[1];
            List<Tuple<Subject, Profile>> results = subjects.stream().map(subject -> {
                final User user = subject.getUser();
                if (user instanceof AnonymousUser) {
                    if (hasAnonymousProfile) {
                        return new Tuple<>(subject, profileFromSubject(subject));
                    } else {
                        return new Tuple<>(subject, (Profile) null);
                    }
                } else if ("reserved".equals(subject.getRealm().getType())) {
                    if (hasReservedProfile) {
                        return new Tuple<>(subject, profileFromSubject(subject));
                    } else {
                        return new Tuple<>(subject, (Profile) null);
                    }
                } else {
                    if (hasNativeProfile) {
                        return new Tuple<>(subject, profileFromSubject(subject));
                    } else {
                        return new Tuple<>(subject, (Profile) null);
                    }
                }
            }).toList();
            if (randomException) {
                assertThat("random exception requires non-empty results", results, not(empty()));
            }
            final Map<Subject, Exception> errors;
            if (randomException) {
                final int exceptionSize = randomIntBetween(1, results.size());
                errors = results.subList(0, exceptionSize)
                    .stream()
                    .collect(Collectors.toUnmodifiableMap(Tuple::v1, t -> new ElasticsearchException("something is not right")));
                results = results.subList(exceptionSize - 1, results.size());
            } else {
                errors = Map.of();
            }
            listener.onResponse(new SubjectSearchResultsAndErrors<>(results, errors));
            return null;
        }).when(profileService).searchProfilesForSubjects(anyList(), anyActionListener());
        return profileService;
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
}
