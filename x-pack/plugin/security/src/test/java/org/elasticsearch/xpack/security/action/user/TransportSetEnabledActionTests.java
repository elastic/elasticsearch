/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
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

/**
 * Unit tests for the {@link TransportSetEnabledAction}
 */
public class TransportSetEnabledActionTests extends ESTestCase {

    public void testAnonymousUser() throws Exception {
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "superuser").build();
        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        Authentication authentication = AuthenticationTestHelper.builder().user(user).build();
        new AuthenticationContextSerializer().writeToContext(authentication, threadContext);

        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        TransportSetEnabledAction action = new TransportSetEnabledAction(
            settings,
            transportService,
            mock(ActionFilters.class),
            securityContext,
            usersStore
        );

        SetEnabledRequest request = new SetEnabledRequest();
        request.username(new AnonymousUser(settings).principal());
        request.enabled(randomBoolean());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ActionResponse.Empty> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty setEnabledResponse) {
                responseRef.set(setEnabledResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(throwableRef.get().getMessage(), containsString("is anonymous"));
        verifyNoMoreInteractions(usersStore);
    }

    public void testValidUser() throws Exception {
        testValidUser(
            randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"), new User(InternalUsers.SYSTEM_USER.principal())),
            defaultAuthentication()
        );
    }

    public void testValidUserWithInternalUsername() throws Exception {
        testValidUser(new User(AuthenticationTestHelper.randomInternalUsername()), defaultAuthentication());
    }

    public void testUserCanModifySameNameUserFromDifferentRealm() throws Exception {
        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new Authentication.RealmRef(randomAlphaOfLengthBetween(3, 8), "other_realm", randomAlphaOfLengthBetween(3, 8)))
            .build(false);
        testValidUser(user, authentication);
    }

    private void testValidUser(User user, Authentication authentication) throws IOException {
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        new AuthenticationContextSerializer().writeToContext(authentication, threadContext);

        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        SetEnabledRequest request = new SetEnabledRequest();
        request.username(user.principal());
        request.enabled(randomBoolean());
        request.setRefreshPolicy(randomFrom(RefreshPolicy.values()));
        // mock the setEnabled call on the native users store so that it will invoke the action listener with a response
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 4;
            @SuppressWarnings("unchecked")
            ActionListener<Void> listener = (ActionListener<Void>) args[3];
            listener.onResponse(null);
            return null;
        }).when(usersStore).setEnabled(eq(user.principal()), eq(request.enabled()), eq(request.getRefreshPolicy()), anyActionListener());
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        TransportSetEnabledAction action = new TransportSetEnabledAction(
            Settings.EMPTY,
            transportService,
            mock(ActionFilters.class),
            securityContext,
            usersStore
        );

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ActionResponse.Empty> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty setEnabledResponse) {
                responseRef.set(setEnabledResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(notNullValue()));
        assertSame(responseRef.get(), ActionResponse.Empty.INSTANCE);
        assertThat(throwableRef.get(), is(nullValue()));
        verify(usersStore, times(1)).setEnabled(
            eq(user.principal()),
            eq(request.enabled()),
            eq(request.getRefreshPolicy()),
            anyActionListener()
        );
    }

    public void testException() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        new AuthenticationContextSerializer().writeToContext(defaultAuthentication(), threadContext);

        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        SetEnabledRequest request = new SetEnabledRequest();
        request.username(user.principal());
        request.enabled(randomBoolean());
        request.setRefreshPolicy(randomFrom(RefreshPolicy.values()));
        final Exception e = randomFrom(new ElasticsearchSecurityException(""), new IllegalStateException(), new RuntimeException());
        // we're mocking the setEnabled call on the native users store so that it will invoke the action listener with an exception
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 4;
            @SuppressWarnings("unchecked")
            ActionListener<Void> listener = (ActionListener<Void>) args[3];
            listener.onFailure(e);
            return null;
        }).when(usersStore).setEnabled(eq(user.principal()), eq(request.enabled()), eq(request.getRefreshPolicy()), anyActionListener());
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        TransportSetEnabledAction action = new TransportSetEnabledAction(
            Settings.EMPTY,
            transportService,
            mock(ActionFilters.class),
            securityContext,
            usersStore
        );

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ActionResponse.Empty> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty setEnabledResponse) {
                responseRef.set(setEnabledResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), is(notNullValue()));
        assertThat(throwableRef.get(), sameInstance(e));
        verify(usersStore, times(1)).setEnabled(
            eq(user.principal()),
            eq(request.enabled()),
            eq(request.getRefreshPolicy()),
            anyActionListener()
        );
    }

    public void testUserModifyingThemselves() throws Exception {
        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        Authentication authentication = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(
                new Authentication.RealmRef(
                    randomAlphaOfLengthBetween(3, 8),
                    randomFrom(NativeRealmSettings.TYPE, ReservedRealm.TYPE),
                    randomAlphaOfLengthBetween(3, 8)
                )
            )
            .build();
        new AuthenticationContextSerializer().writeToContext(authentication, threadContext);

        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        SetEnabledRequest request = new SetEnabledRequest();
        request.username(user.principal());
        request.enabled(randomBoolean());
        request.setRefreshPolicy(randomFrom(RefreshPolicy.values()));
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        TransportSetEnabledAction action = new TransportSetEnabledAction(
            Settings.EMPTY,
            transportService,
            mock(ActionFilters.class),
            securityContext,
            usersStore
        );

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ActionResponse.Empty> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty setEnabledResponse) {
                responseRef.set(setEnabledResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(throwableRef.get().getMessage(), containsString("own account"));
        verifyNoMoreInteractions(usersStore);
    }

    private Authentication defaultAuthentication() throws IOException {
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("the runner"))
            .realmRef(
                new Authentication.RealmRef(randomAlphaOfLengthBetween(3, 8), NativeRealmSettings.TYPE, randomAlphaOfLengthBetween(3, 8))
            )
            .build();
        return authentication;
    }
}
