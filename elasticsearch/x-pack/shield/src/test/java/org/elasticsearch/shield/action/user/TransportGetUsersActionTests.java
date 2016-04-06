/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.esnative.NativeUsersStore;
import org.elasticsearch.shield.authc.esnative.ReservedRealm;
import org.elasticsearch.shield.user.AnonymousUser;
import org.elasticsearch.shield.user.SystemUser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TransportGetUsersActionTests extends ESTestCase {

    private boolean anonymousEnabled;

    @Before
    public void maybeEnableAnonymous() {
        anonymousEnabled = randomBoolean();
        if (anonymousEnabled) {
            Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "superuser").build();
            AnonymousUser.initialize(settings);
        }
    }

    @After
    public void resetAnonymous() {
        AnonymousUser.initialize(Settings.EMPTY);
    }

    public void testAnonymousUser() {
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportGetUsersAction action = new TransportGetUsersAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore, mock(TransportService.class));

        GetUsersRequest request = new GetUsersRequest();
        request.usernames(AnonymousUser.INSTANCE.principal());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetUsersResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<GetUsersResponse>() {
            @Override
            public void onResponse(GetUsersResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        if (anonymousEnabled) {
            assertThat(responseRef.get().users(), arrayContaining(AnonymousUser.INSTANCE));
        } else {
            assertThat(responseRef.get().users(), emptyArray());
        }
        verifyZeroInteractions(usersStore);
    }

    public void testSystemUser() {
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportGetUsersAction action = new TransportGetUsersAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore, mock(TransportService.class));

        GetUsersRequest request = new GetUsersRequest();
        request.usernames(SystemUser.INSTANCE.principal());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetUsersResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<GetUsersResponse>() {
            @Override
            public void onResponse(GetUsersResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        assertThat(throwableRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(throwableRef.get().getMessage(), containsString("is internal"));
        assertThat(responseRef.get(), is(nullValue()));
        verifyZeroInteractions(usersStore);
    }

    public void testReservedUsersOnly() {
        final int size = randomIntBetween(1, ReservedRealm.users().size());
        final List<User> reservedUsers = randomSubsetOf(size, ReservedRealm.users());
        final List<String> names = reservedUsers.stream().map(User::principal).collect(Collectors.toList());
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportGetUsersAction action = new TransportGetUsersAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore, mock(TransportService.class));

        GetUsersRequest request = new GetUsersRequest();
        request.usernames(names.toArray(new String[names.size()]));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetUsersResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<GetUsersResponse>() {
            @Override
            public void onResponse(GetUsersResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(responseRef.get().users(), arrayContaining(reservedUsers.toArray(new User[reservedUsers.size()])));
        verifyZeroInteractions(usersStore);
    }

    public void testGetAllUsers() {
        final List<User> storeUsers = randomFrom(Collections.<User>emptyList(), Collections.singletonList(new User("joe")),
                Arrays.asList(new User("jane"), new User("fred")), randomUsers());
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportGetUsersAction action = new TransportGetUsersAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore, mock(TransportService.class));

        GetUsersRequest request = new GetUsersRequest();
        doAnswer(new Answer() {
            public Void answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                assert args.length == 2;
                ActionListener<List<User>> listener = (ActionListener<List<User>>) args[1];
                listener.onResponse(storeUsers);
                return null;
            }
        }).when(usersStore).getUsers(eq(Strings.EMPTY_ARRAY), any(ActionListener.class));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetUsersResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<GetUsersResponse>() {
            @Override
            public void onResponse(GetUsersResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        final List<User> expectedList = new ArrayList<>();
        expectedList.addAll(ReservedRealm.users());
        expectedList.addAll(storeUsers);

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(responseRef.get().users(), arrayContaining(expectedList.toArray(new User[expectedList.size()])));
        verify(usersStore, times(1)).getUsers(aryEq(Strings.EMPTY_ARRAY), any(ActionListener.class));
    }

    public void testGetStoreOnlyUsers() {
        final List<User> storeUsers =
                randomFrom(Collections.singletonList(new User("joe")), Arrays.asList(new User("jane"), new User("fred")), randomUsers());
        final String[] storeUsernames = storeUsers.stream().map(User::principal).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY);
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportGetUsersAction action = new TransportGetUsersAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore, mock(TransportService.class));

        GetUsersRequest request = new GetUsersRequest();
        request.usernames(storeUsernames);
        if (storeUsernames.length > 1) {
            doAnswer(new Answer() {
                public Void answer(InvocationOnMock invocation) {
                    Object[] args = invocation.getArguments();
                    assert args.length == 2;
                    ActionListener<List<User>> listener = (ActionListener<List<User>>) args[1];
                    listener.onResponse(storeUsers);
                    return null;
                }
            }).when(usersStore).getUsers(aryEq(storeUsernames), any(ActionListener.class));
        } else {
            assertThat(storeUsernames.length, is(1));
            doAnswer(new Answer() {
                public Void answer(InvocationOnMock invocation) {
                    Object[] args = invocation.getArguments();
                    assert args.length == 2;
                    ActionListener<User> listener = (ActionListener<User>) args[1];
                    listener.onResponse(storeUsers.get(0));
                    return null;
                }
            }).when(usersStore).getUser(eq(storeUsernames[0]), any(ActionListener.class));
        }

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetUsersResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<GetUsersResponse>() {
            @Override
            public void onResponse(GetUsersResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        final List<User> expectedList = new ArrayList<>();
        expectedList.addAll(storeUsers);

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(responseRef.get().users(), arrayContaining(expectedList.toArray(new User[expectedList.size()])));
        if (storeUsers.size() > 1) {
            verify(usersStore, times(1)).getUsers(aryEq(storeUsernames), any(ActionListener.class));
        } else {
            verify(usersStore, times(1)).getUser(eq(storeUsernames[0]), any(ActionListener.class));
        }
    }

    public void testException() {
        final Throwable t = randomFrom(new ElasticsearchSecurityException(""), new IllegalStateException(), new ValidationException());
        final List<User> storeUsers =
                randomFrom(Collections.singletonList(new User("joe")), Arrays.asList(new User("jane"), new User("fred")), randomUsers());
        final String[] storeUsernames = storeUsers.stream().map(User::principal).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY);
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportGetUsersAction action = new TransportGetUsersAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore, mock(TransportService.class));

        GetUsersRequest request = new GetUsersRequest();
        request.usernames(storeUsernames);
        if (storeUsernames.length > 1) {
            doAnswer(new Answer() {
                public Void answer(InvocationOnMock invocation) {
                    Object[] args = invocation.getArguments();
                    assert args.length == 2;
                    ActionListener<List<User>> listener = (ActionListener<List<User>>) args[1];
                    listener.onFailure(t);
                    return null;
                }
            }).when(usersStore).getUsers(aryEq(storeUsernames), any(ActionListener.class));
        } else {
            assertThat(storeUsernames.length, is(1));
            doAnswer(new Answer() {
                public Void answer(InvocationOnMock invocation) {
                    Object[] args = invocation.getArguments();
                    assert args.length == 2;
                    ActionListener<User> listener = (ActionListener<User>) args[1];
                    listener.onFailure(t);
                    return null;
                }
            }).when(usersStore).getUser(eq(storeUsernames[0]), any(ActionListener.class));
        }

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<GetUsersResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<GetUsersResponse>() {
            @Override
            public void onResponse(GetUsersResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        assertThat(throwableRef.get(), is(notNullValue()));
        assertThat(throwableRef.get(), is(sameInstance(t)));
        assertThat(responseRef.get(), is(nullValue()));
        if (request.usernames().length == 1) {
            verify(usersStore, times(1)).getUser(eq(request.usernames()[0]), any(ActionListener.class));
        } else {
            verify(usersStore, times(1)).getUsers(aryEq(storeUsernames), any(ActionListener.class));
        }
    }

    private List<User> randomUsers() {
        int size = scaledRandomIntBetween(3, 16);
        List<User> users = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            users.add(new User("user_" + i, randomAsciiOfLengthBetween(4, 12)));
        }
        return users;
    }
}
