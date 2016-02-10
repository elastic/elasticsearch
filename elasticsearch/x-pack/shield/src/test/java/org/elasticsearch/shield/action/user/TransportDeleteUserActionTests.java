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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TransportDeleteUserActionTests extends ESTestCase {

    @After
    public void resetAnonymous() {
        AnonymousUser.initialize(Settings.EMPTY);
    }

    public void testAnonymousUser() {
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "superuser").build();
        AnonymousUser.initialize(settings);
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportDeleteUserAction action = new TransportDeleteUserAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore, mock(TransportService.class));

        DeleteUserRequest request = new DeleteUserRequest(AnonymousUser.INSTANCE.principal());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<DeleteUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<DeleteUserResponse>() {
            @Override
            public void onResponse(DeleteUserResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(throwableRef.get().getMessage(), containsString("is anonymous and cannot be deleted"));
        verifyZeroInteractions(usersStore);
    }

    public void testSystemUser() {
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportDeleteUserAction action = new TransportDeleteUserAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore, mock(TransportService.class));

        DeleteUserRequest request = new DeleteUserRequest(SystemUser.INSTANCE.principal());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<DeleteUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<DeleteUserResponse>() {
            @Override
            public void onResponse(DeleteUserResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(throwableRef.get().getMessage(), containsString("is internal"));
        verifyZeroInteractions(usersStore);
    }

    public void testReservedUser() {
        final User reserved = randomFrom(ReservedRealm.users().toArray(new User[0]));
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportDeleteUserAction action = new TransportDeleteUserAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore, mock(TransportService.class));

        DeleteUserRequest request = new DeleteUserRequest(reserved.principal());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<DeleteUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<DeleteUserResponse>() {
            @Override
            public void onResponse(DeleteUserResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(throwableRef.get().getMessage(), containsString("is reserved and cannot be deleted"));
        verifyZeroInteractions(usersStore);
    }

    public void testValidUser() {
        final User user = new User("joe");
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportDeleteUserAction action = new TransportDeleteUserAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore, mock(TransportService.class));

        final boolean found = randomBoolean();
        final DeleteUserRequest request = new DeleteUserRequest(user.principal());
        doAnswer(new Answer() {
            public Void answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                assert args.length == 2;
                ActionListener<Boolean> listener = (ActionListener<Boolean>) args[1];
                listener.onResponse(found);
                return null;
            }
        }).when(usersStore).deleteUser(eq(request), any(ActionListener.class));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<DeleteUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<DeleteUserResponse>() {
            @Override
            public void onResponse(DeleteUserResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(responseRef.get().found(), is(found));
        assertThat(throwableRef.get(), is(nullValue()));
        verify(usersStore, times(1)).deleteUser(eq(request), any(ActionListener.class));
    }

    public void testException() {
        final Throwable t = randomFrom(new ElasticsearchSecurityException(""), new IllegalStateException(), new RuntimeException());
        final User user = new User("joe");
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportDeleteUserAction action = new TransportDeleteUserAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore, mock(TransportService.class));

        final DeleteUserRequest request = new DeleteUserRequest(user.principal());
        doAnswer(new Answer() {
            public Void answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                assert args.length == 2;
                ActionListener<Boolean> listener = (ActionListener<Boolean>) args[1];
                listener.onFailure(t);
                return null;
            }
        }).when(usersStore).deleteUser(eq(request), any(ActionListener.class));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<DeleteUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<DeleteUserResponse>() {
            @Override
            public void onResponse(DeleteUserResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Throwable e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), is(notNullValue()));
        assertThat(throwableRef.get(), sameInstance(t));
        verify(usersStore, times(1)).deleteUser(eq(request), any(ActionListener.class));
    }
}
