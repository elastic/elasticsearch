/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.GetUsersRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUsersResponse;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealmTests;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TransportGetUsersActionTests extends ESTestCase {

    private boolean anonymousEnabled;
    private Settings settings;
    private ThreadPool threadPool;

    @Before
    public void maybeEnableAnonymous() {
        anonymousEnabled = randomBoolean();
        if (anonymousEnabled) {
            settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "superuser").build();
        } else {
            settings = Settings.EMPTY;
        }
        threadPool = new TestThreadPool("TransportGetUsersActionTests");
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
        ReservedRealm reservedRealm =
            new ReservedRealm(mock(Environment.class), settings, usersStore, anonymousUser, securityIndex, threadPool);
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        TransportGetUsersAction action = new TransportGetUsersAction(Settings.EMPTY, mock(ActionFilters.class),
                usersStore, transportService, reservedRealm);

        GetUsersRequest request = new GetUsersRequest();
        request.usernames(anonymousUser.principal());

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
        verifyZeroInteractions(usersStore);
    }

    public void testInternalUser() {
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        TransportGetUsersAction action = new TransportGetUsersAction(Settings.EMPTY, mock(ActionFilters.class),
                usersStore, transportService, mock(ReservedRealm.class));

        GetUsersRequest request = new GetUsersRequest();
        request.usernames(randomFrom(SystemUser.INSTANCE.principal(), XPackUser.INSTANCE.principal(),
            XPackSecurityUser.INSTANCE.principal(), AsyncSearchUser.INSTANCE.principal()));

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

        assertThat(throwableRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(throwableRef.get().getMessage(), containsString("is internal"));
        assertThat(responseRef.get(), is(nullValue()));
        verifyZeroInteractions(usersStore);
    }

    public void testReservedUsersOnly() {
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.isAvailable()).thenReturn(true);
        when(securityIndex.checkMappingVersion(any())).thenReturn(true);

        ReservedRealmTests.mockGetAllReservedUserInfo(usersStore, Collections.emptyMap());
        ReservedRealm reservedRealm =
            new ReservedRealm(mock(Environment.class), settings, usersStore, new AnonymousUser(settings), securityIndex, threadPool);
        PlainActionFuture<Collection<User>> userFuture = new PlainActionFuture<>();
        reservedRealm.users(userFuture);
        final Collection<User> allReservedUsers = userFuture.actionGet();
        final int size = randomIntBetween(1, allReservedUsers.size());
        final List<User> reservedUsers = randomSubsetOf(size, allReservedUsers);
        final List<String> names = reservedUsers.stream().map(User::principal).collect(Collectors.toList());
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        TransportGetUsersAction action = new TransportGetUsersAction(Settings.EMPTY, mock(ActionFilters.class),
                usersStore, transportService, reservedRealm);

        logger.error("names {}", names);
        GetUsersRequest request = new GetUsersRequest();
        request.usernames(names.toArray(new String[names.size()]));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetUsersResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<GetUsersResponse>() {
            @Override
            public void onResponse(GetUsersResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Request failed",  e);
                throwableRef.set(e);
            }
        });

        User[] users = responseRef.get().users();

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(users, arrayContaining(reservedUsers.toArray(new User[reservedUsers.size()])));
    }

    public void testGetAllUsers() {
        final List<User> storeUsers = randomFrom(Collections.<User>emptyList(), Collections.singletonList(new User("joe")),
                Arrays.asList(new User("jane"), new User("fred")), randomUsers());
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.isAvailable()).thenReturn(true);
        ReservedRealmTests.mockGetAllReservedUserInfo(usersStore, Collections.emptyMap());
        ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), settings, usersStore, new AnonymousUser(settings),
                securityIndex, threadPool);
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        TransportGetUsersAction action = new TransportGetUsersAction(Settings.EMPTY, mock(ActionFilters.class),
                usersStore, transportService, reservedRealm);

        GetUsersRequest request = new GetUsersRequest();
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
        PlainActionFuture<Collection<User>> userFuture = new PlainActionFuture<>();
        reservedRealm.users(userFuture);
        expectedList.addAll(userFuture.actionGet());
        expectedList.addAll(storeUsers);

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(responseRef.get().users(), arrayContaining(expectedList.toArray(new User[expectedList.size()])));
        verify(usersStore, times(1)).getUsers(aryEq(Strings.EMPTY_ARRAY), anyActionListener());
    }

    public void testGetStoreOnlyUsers() {
        final List<User> storeUsers =
                randomFrom(Collections.singletonList(new User("joe")), Arrays.asList(new User("jane"), new User("fred")), randomUsers());
        final String[] storeUsernames = storeUsers.stream().map(User::principal).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY);
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        TransportGetUsersAction action = new TransportGetUsersAction(Settings.EMPTY, mock(ActionFilters.class),
                usersStore, transportService, mock(ReservedRealm.class));

        GetUsersRequest request = new GetUsersRequest();
        request.usernames(storeUsernames);
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
        if (storeUsers.size() > 1) {
            verify(usersStore, times(1)).getUsers(aryEq(storeUsernames), anyActionListener());
        } else {
            verify(usersStore, times(1)).getUsers(aryEq(new String[] {storeUsernames[0]}), anyActionListener());
        }
    }

    public void testException() {
        final Exception e = randomFrom(new ElasticsearchSecurityException(""), new IllegalStateException(), new ValidationException());
        final List<User> storeUsers =
                randomFrom(Collections.singletonList(new User("joe")), Arrays.asList(new User("jane"), new User("fred")), randomUsers());
        final String[] storeUsernames = storeUsers.stream().map(User::principal).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY);
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        TransportGetUsersAction action = new TransportGetUsersAction(Settings.EMPTY, mock(ActionFilters.class),
                usersStore, transportService, mock(ReservedRealm.class));

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
}
