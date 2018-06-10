/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.HasherFactory;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealmTests;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.Collections;
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
import static org.mockito.Mockito.when;

public class TransportPutUserActionTests extends ESTestCase {

    public void testAnonymousUser() {
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "superuser").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, null, null, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> null, null, Collections.emptySet());
        TransportPutUserAction action = new TransportPutUserAction(settings, mock(ThreadPool.class), mock(ActionFilters.class),
                mock(IndexNameExpressionResolver.class), usersStore, transportService);

        PutUserRequest request = new PutUserRequest();
        request.username(anonymousUser.principal());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<PutUserResponse>() {
            @Override
            public void onResponse(PutUserResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(throwableRef.get().getMessage(), containsString("is anonymous and cannot be modified"));
        verifyZeroInteractions(usersStore);
    }

    public void testSystemUser() {
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, null, null, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> null, null, Collections.emptySet());
        TransportPutUserAction action = new TransportPutUserAction(Settings.EMPTY, mock(ThreadPool.class), mock(ActionFilters.class),
                mock(IndexNameExpressionResolver.class), usersStore, transportService);

        PutUserRequest request = new PutUserRequest();
        request.username(randomFrom(SystemUser.INSTANCE.principal(), XPackUser.INSTANCE.principal()));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<PutUserResponse>() {
            @Override
            public void onResponse(PutUserResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(throwableRef.get().getMessage(), containsString("is internal"));
        verifyZeroInteractions(usersStore);
    }

    public void testReservedUser() {
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.isAvailable()).thenReturn(true);
        ReservedRealmTests.mockGetAllReservedUserInfo(usersStore, Collections.emptyMap());
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(settings));
        ReservedRealm reservedRealm = new ReservedRealm(TestEnvironment.newEnvironment(settings), settings, usersStore,
                                                        new AnonymousUser(settings), securityIndex, threadPool);
        PlainActionFuture<Collection<User>> userFuture = new PlainActionFuture<>();
        reservedRealm.users(userFuture);
        final User reserved = randomFrom(userFuture.actionGet().toArray(new User[0]));
        TransportService transportService = new TransportService(Settings.EMPTY, null, null, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> null, null, Collections.emptySet());
        TransportPutUserAction action = new TransportPutUserAction(Settings.EMPTY, threadPool, mock(ActionFilters.class),
                mock(IndexNameExpressionResolver.class), usersStore, transportService);

        PutUserRequest request = new PutUserRequest();
        request.username(reserved.principal());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<PutUserResponse>() {
            @Override
            public void onResponse(PutUserResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(throwableRef.get().getMessage(), containsString("is reserved and only the password"));
    }

    public void testValidUser() {
        final User user = new User("joe");
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, null, null, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> null, null, Collections.emptySet());
        TransportPutUserAction action = new TransportPutUserAction(Settings.EMPTY, mock(ThreadPool.class), mock(ActionFilters.class),
                mock(IndexNameExpressionResolver.class), usersStore, transportService);

        final boolean isCreate = randomBoolean();
        final PutUserRequest request = new PutUserRequest();
        final Hasher hasher = HasherFactory.getHasher(SecuritySettingsSource.HASHING_ALGORITHM);
        request.username(user.principal());
        if (isCreate) {
            request.passwordHash(hasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        }
        final boolean created = isCreate ? randomBoolean() : false; // updates should always return false for create
        doAnswer(new Answer() {
            public Void answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                assert args.length == 2;
                ActionListener<Boolean> listener = (ActionListener<Boolean>) args[1];
                listener.onResponse(created);
                return null;
            }
        }).when(usersStore).putUser(eq(request), any(ActionListener.class));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<PutUserResponse>() {
            @Override
            public void onResponse(PutUserResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(responseRef.get().created(), is(created));
        assertThat(throwableRef.get(), is(nullValue()));
        verify(usersStore, times(1)).putUser(eq(request), any(ActionListener.class));
    }

    public void testException() {
        final Exception e = randomFrom(new ElasticsearchSecurityException(""), new IllegalStateException(), new ValidationException());
        final User user = new User("joe");
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, null, null, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> null, null, Collections.emptySet());
        TransportPutUserAction action = new TransportPutUserAction(Settings.EMPTY, mock(ThreadPool.class), mock(ActionFilters.class),
                mock(IndexNameExpressionResolver.class), usersStore, transportService);

        final PutUserRequest request = new PutUserRequest();
        request.username(user.principal());
        doAnswer(new Answer() {
            public Void answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                assert args.length == 2;
                ActionListener<Boolean> listener = (ActionListener<Boolean>) args[1];
                listener.onFailure(e);
                return null;
            }
        }).when(usersStore).putUser(eq(request), any(ActionListener.class));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<PutUserResponse>() {
            @Override
            public void onResponse(PutUserResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), is(notNullValue()));
        assertThat(throwableRef.get(), sameInstance(e));
        verify(usersStore, times(1)).putUser(eq(request), any(ActionListener.class));
    }
}
