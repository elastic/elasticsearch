/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordResponse;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.HasherFactory;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.Security;
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

public class TransportChangePasswordActionTests extends ESTestCase {

    public void testAnonymousUser() {
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "superuser").build();
        AnonymousUser anonymousUser = new AnonymousUser(settings);
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, null, null, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> null, null, Collections.emptySet());
        TransportChangePasswordAction action = new TransportChangePasswordAction(settings, mock(ThreadPool.class), transportService,
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore);
        final Hasher hasher = HasherFactory.getHasher(SecuritySettingsSource.HASHING_ALGORITHM);
        ChangePasswordRequest request = new ChangePasswordRequest();
        request.username(anonymousUser.principal());
        request.passwordHash(hasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ChangePasswordResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<ChangePasswordResponse>() {
            @Override
            public void onResponse(ChangePasswordResponse changePasswordResponse) {
                responseRef.set(changePasswordResponse);
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

    public void testInternalUsers() {
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, null, null, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> null, null, Collections.emptySet());
        TransportChangePasswordAction action = new TransportChangePasswordAction(Settings.EMPTY, mock(ThreadPool.class), transportService,
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore);
        final Hasher hasher = HasherFactory.getHasher(SecuritySettingsSource.HASHING_ALGORITHM);
        ChangePasswordRequest request = new ChangePasswordRequest();
        request.username(randomFrom(SystemUser.INSTANCE.principal(), XPackUser.INSTANCE.principal()));
        request.passwordHash(hasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ChangePasswordResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<ChangePasswordResponse>() {
            @Override
            public void onResponse(ChangePasswordResponse changePasswordResponse) {
                responseRef.set(changePasswordResponse);
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

    public void testValidUser() {
        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        final Hasher hasher = HasherFactory.getHasher("pbkdf2");
        ChangePasswordRequest request = new ChangePasswordRequest();
        request.username(user.principal());
        request.passwordHash(hasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            ActionListener<Void> listener = (ActionListener<Void>) args[1];
            listener.onResponse(null);
            return null;
        }).when(usersStore).changePassword(eq(request), any(ActionListener.class));
        TransportService transportService = new TransportService(Settings.EMPTY, null, null, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> null, null, Collections.emptySet());
        TransportChangePasswordAction action = new TransportChangePasswordAction(Settings.EMPTY, mock(ThreadPool.class), transportService,
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore);

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ChangePasswordResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<ChangePasswordResponse>() {
            @Override
            public void onResponse(ChangePasswordResponse changePasswordResponse) {
                responseRef.set(changePasswordResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(notNullValue()));
        assertThat(responseRef.get(), instanceOf(ChangePasswordResponse.class));
        assertThat(throwableRef.get(), is(nullValue()));
        verify(usersStore, times(1)).changePassword(eq(request), any(ActionListener.class));
    }

    public void testException() {
        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        ChangePasswordRequest request = new ChangePasswordRequest();
        final Hasher hasher = HasherFactory.getHasher(SecuritySettingsSource.HASHING_ALGORITHM);
        request.username(user.principal());
        request.passwordHash(hasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        final Exception e = randomFrom(new ElasticsearchSecurityException(""), new IllegalStateException(), new RuntimeException());
        doAnswer(new Answer() {
            public Void answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                assert args.length == 2;
                ActionListener<Void> listener = (ActionListener<Void>) args[1];
                listener.onFailure(e);
                return null;
            }
        }).when(usersStore).changePassword(eq(request), any(ActionListener.class));
        TransportService transportService = new TransportService(Settings.EMPTY, null, null, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> null, null, Collections.emptySet());
        TransportChangePasswordAction action = new TransportChangePasswordAction(Settings.EMPTY, mock(ThreadPool.class), transportService,
                mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), usersStore);

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ChangePasswordResponse> responseRef = new AtomicReference<>();
        action.doExecute(request, new ActionListener<ChangePasswordResponse>() {
            @Override
            public void onResponse(ChangePasswordResponse changePasswordResponse) {
                responseRef.set(changePasswordResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), is(notNullValue()));
        assertThat(throwableRef.get(), sameInstance(e));
        verify(usersStore, times(1)).changePassword(eq(request), any(ActionListener.class));
    }
}
