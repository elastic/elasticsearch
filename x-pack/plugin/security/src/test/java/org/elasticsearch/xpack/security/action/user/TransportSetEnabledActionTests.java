/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.xpack.core.security.action.user.SetEnabledResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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
        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.encode()).thenReturn(randomAlphaOfLength(24)); // just can't be null
        new AuthenticationContextSerializer().writeToContext(authentication, threadContext);

        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        TransportSetEnabledAction action = new TransportSetEnabledAction(settings, threadPool, transportService, mock(ActionFilters.class),
                securityContext, usersStore);

        SetEnabledRequest request = new SetEnabledRequest();
        request.username(new AnonymousUser(settings).principal());
        request.enabled(randomBoolean());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<SetEnabledResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<SetEnabledResponse>() {
            @Override
            public void onResponse(SetEnabledResponse setEnabledResponse) {
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
        verifyZeroInteractions(usersStore);
    }

    public void testInternalUser() throws Exception {
        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.encode()).thenReturn(randomAlphaOfLength(24)); // just can't be null
        new AuthenticationContextSerializer().writeToContext(authentication, threadContext);

        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        TransportSetEnabledAction action = new TransportSetEnabledAction(Settings.EMPTY, threadPool, transportService,
                mock(ActionFilters.class), securityContext, usersStore);

        SetEnabledRequest request = new SetEnabledRequest();
        request.username(randomFrom(SystemUser.INSTANCE.principal(), XPackUser.INSTANCE.principal()));
        request.enabled(randomBoolean());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<SetEnabledResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<SetEnabledResponse>() {
            @Override
            public void onResponse(SetEnabledResponse setEnabledResponse) {
                responseRef.set(setEnabledResponse);
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

    public void testValidUser() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(new User("the runner"));
        when(authentication.encode()).thenReturn(randomAlphaOfLength(24)); // just can't be null
        new AuthenticationContextSerializer().writeToContext(authentication, threadContext);

        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        SetEnabledRequest request = new SetEnabledRequest();
        request.username(user.principal());
        request.enabled(randomBoolean());
        request.setRefreshPolicy(randomFrom(RefreshPolicy.values()));
        // mock the setEnabled call on the native users store so that it will invoke the action listener with a response
        doAnswer(new Answer() {
            public Void answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                assert args.length == 4;
                ActionListener<Void> listener = (ActionListener<Void>) args[3];
                listener.onResponse(null);
                return null;
            }
        }).when(usersStore)
                .setEnabled(eq(user.principal()), eq(request.enabled()), eq(request.getRefreshPolicy()), any(ActionListener.class));
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        TransportSetEnabledAction action = new TransportSetEnabledAction(Settings.EMPTY, threadPool, transportService,
                mock(ActionFilters.class), securityContext, usersStore);

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<SetEnabledResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<SetEnabledResponse>() {
            @Override
            public void onResponse(SetEnabledResponse setEnabledResponse) {
                responseRef.set(setEnabledResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(responseRef.get(), instanceOf(SetEnabledResponse.class));
        assertThat(throwableRef.get(), is(nullValue()));
        verify(usersStore, times(1))
                .setEnabled(eq(user.principal()), eq(request.enabled()), eq(request.getRefreshPolicy()), any(ActionListener.class));
    }

    public void testException() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(new User("the runner"));
        when(authentication.encode()).thenReturn(randomAlphaOfLength(24)); // just can't be null
        new AuthenticationContextSerializer().writeToContext(authentication, threadContext);

        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        SetEnabledRequest request = new SetEnabledRequest();
        request.username(user.principal());
        request.enabled(randomBoolean());
        request.setRefreshPolicy(randomFrom(RefreshPolicy.values()));
        final Exception e = randomFrom(new ElasticsearchSecurityException(""), new IllegalStateException(), new RuntimeException());
        // we're mocking the setEnabled call on the native users store so that it will invoke the action listener with an exception
        doAnswer(new Answer() {
            public Void answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                assert args.length == 4;
                ActionListener<Void> listener = (ActionListener<Void>) args[3];
                listener.onFailure(e);
                return null;
            }
        }).when(usersStore)
                .setEnabled(eq(user.principal()), eq(request.enabled()), eq(request.getRefreshPolicy()), any(ActionListener.class));
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        TransportSetEnabledAction action = new TransportSetEnabledAction(Settings.EMPTY, threadPool, transportService,
            mock(ActionFilters.class), securityContext, usersStore);

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<SetEnabledResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<SetEnabledResponse>() {
            @Override
            public void onResponse(SetEnabledResponse setEnabledResponse) {
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
        verify(usersStore, times(1))
                .setEnabled(eq(user.principal()), eq(request.enabled()), eq(request.getRefreshPolicy()), any(ActionListener.class));
    }

    public void testUserModifyingThemselves() throws Exception {
        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        Authentication authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.encode()).thenReturn(randomAlphaOfLength(24)); // just can't be null
        new AuthenticationContextSerializer().writeToContext(authentication, threadContext);

        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        SetEnabledRequest request = new SetEnabledRequest();
        request.username(user.principal());
        request.enabled(randomBoolean());
        request.setRefreshPolicy(randomFrom(RefreshPolicy.values()));
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        TransportSetEnabledAction action = new TransportSetEnabledAction(Settings.EMPTY, threadPool, transportService,
                mock(ActionFilters.class), securityContext, usersStore);

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<SetEnabledResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<SetEnabledResponse>() {
            @Override
            public void onResponse(SetEnabledResponse setEnabledResponse) {
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
        verifyZeroInteractions(usersStore);
    }
}
