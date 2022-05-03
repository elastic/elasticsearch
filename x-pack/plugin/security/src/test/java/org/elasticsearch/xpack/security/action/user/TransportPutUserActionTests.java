/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealmTests;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.SecurityIntegTestCase.getFastStoredHashAlgoForTests;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TransportPutUserActionTests extends ESTestCase {

    public void testAnonymousUser() {
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "superuser").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
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
        TransportPutUserAction action = new TransportPutUserAction(settings, mock(ActionFilters.class), usersStore, transportService);

        PutUserRequest request = new PutUserRequest();
        request.username(anonymousUser.principal());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<PutUserResponse>() {
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
        verifyNoMoreInteractions(usersStore);
    }

    public void testSystemUser() {
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
        TransportPutUserAction action = new TransportPutUserAction(Settings.EMPTY, mock(ActionFilters.class), usersStore, transportService);

        PutUserRequest request = new PutUserRequest();
        request.username(
            randomFrom(
                SystemUser.INSTANCE.principal(),
                XPackUser.INSTANCE.principal(),
                XPackSecurityUser.INSTANCE.principal(),
                AsyncSearchUser.INSTANCE.principal()
            )
        );

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<PutUserResponse>() {
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
        verifyNoMoreInteractions(usersStore);
    }

    public void testReservedUser() {
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        ReservedRealmTests.mockGetAllReservedUserInfo(usersStore, Collections.emptyMap());
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(settings));
        ReservedRealm reservedRealm = new ReservedRealm(
            TestEnvironment.newEnvironment(settings),
            settings,
            usersStore,
            new AnonymousUser(settings),
            threadPool
        );
        PlainActionFuture<Collection<User>> userFuture = new PlainActionFuture<>();
        reservedRealm.users(userFuture);
        final User reserved = randomFrom(userFuture.actionGet().toArray(new User[0]));
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportPutUserAction action = new TransportPutUserAction(Settings.EMPTY, mock(ActionFilters.class), usersStore, transportService);

        PutUserRequest request = new PutUserRequest();
        request.username(reserved.principal());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<PutUserResponse>() {
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
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportPutUserAction action = new TransportPutUserAction(Settings.EMPTY, mock(ActionFilters.class), usersStore, transportService);

        final boolean isCreate = randomBoolean();
        final PutUserRequest request = new PutUserRequest();
        request.username(user.principal());
        final Hasher hasher = getFastStoredHashAlgoForTests();
        if (isCreate) {
            request.passwordHash(hasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        }
        final boolean created = isCreate ? randomBoolean() : false; // updates should always return false for create
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> listener = (ActionListener<Boolean>) args[1];
            listener.onResponse(created);
            return null;
        }).when(usersStore).putUser(eq(request), anyActionListener());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<PutUserResponse>() {
            @Override
            public void onResponse(PutUserResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(throwableRef.get(), is(nullValue()));
        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(responseRef.get().created(), is(created));
        verify(usersStore, times(1)).putUser(eq(request), anyActionListener());
    }

    public void testInvalidUser() {
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
        TransportPutUserAction action = new TransportPutUserAction(Settings.EMPTY, mock(ActionFilters.class), usersStore, transportService);

        final PutUserRequest request = new PutUserRequest();
        request.username("fóóbár");
        request.roles("bar");
        ActionRequestValidationException validation = request.validate();
        assertNull(validation);

        PlainActionFuture<PutUserResponse> responsePlainActionFuture = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, responsePlainActionFuture);
        validation = expectThrows(ActionRequestValidationException.class, responsePlainActionFuture::actionGet);
        assertThat(validation.validationErrors(), contains(containsString("must be")));
        assertThat(validation.validationErrors().size(), is(1));
    }

    public void testException() {
        final Exception e = randomFrom(new ElasticsearchSecurityException(""), new IllegalStateException(), new ValidationException());
        final User user = new User("joe");
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
        TransportPutUserAction action = new TransportPutUserAction(Settings.EMPTY, mock(ActionFilters.class), usersStore, transportService);

        final PutUserRequest request = new PutUserRequest();
        request.username(user.principal());
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> listener = (ActionListener<Boolean>) args[1];
            listener.onFailure(e);
            return null;
        }).when(usersStore).putUser(eq(request), anyActionListener());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<PutUserResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<PutUserResponse>() {
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
        verify(usersStore, times(1)).putUser(eq(request), anyActionListener());
    }
}
