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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.List;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportAuthenticateActionTests extends ESTestCase {

    public void testInternalUser() {
        SecurityContext securityContext = mock(SecurityContext.class);
        final Authentication authentication = new Authentication(
            randomFrom(SystemUser.INSTANCE, XPackUser.INSTANCE, XPackSecurityUser.INSTANCE, AsyncSearchUser.INSTANCE),
            new Authentication.RealmRef("native", "default_native", "node1"),
            null
        );
        when(securityContext.getAuthentication()).thenReturn(authentication);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportAuthenticateAction action = new TransportAuthenticateAction(
            transportService,
            mock(ActionFilters.class),
            securityContext,
            prepareAnonymousUser()
        );

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<AuthenticateResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), new AuthenticateRequest(), new ActionListener<AuthenticateResponse>() {
            @Override
            public void onResponse(AuthenticateResponse authenticateResponse) {
                responseRef.set(authenticateResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), nullValue());
        assertThat(throwableRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(throwableRef.get().getMessage(), containsString("is internal"));
    }

    public void testNullUser() {
        SecurityContext securityContext = mock(SecurityContext.class);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportAuthenticateAction action = new TransportAuthenticateAction(
            transportService,
            mock(ActionFilters.class),
            securityContext,
            prepareAnonymousUser()
        );

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<AuthenticateResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), new AuthenticateRequest(), new ActionListener<AuthenticateResponse>() {
            @Override
            public void onResponse(AuthenticateResponse authenticateResponse) {
                responseRef.set(authenticateResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), nullValue());
        assertThat(throwableRef.get(), instanceOf(ElasticsearchSecurityException.class));
        assertThat(throwableRef.get().getMessage(), containsString("did not find an authenticated user"));
    }

    public void testValidAuthentication() {
        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        final Authentication authentication = new Authentication(
            user,
            new Authentication.RealmRef("native_realm", "native", "node1"),
            null
        );
        SecurityContext securityContext = mock(SecurityContext.class);
        when(securityContext.getAuthentication()).thenReturn(authentication);
        when(securityContext.getUser()).thenReturn(user);

        final AnonymousUser anonymousUser = prepareAnonymousUser();
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportAuthenticateAction action = new TransportAuthenticateAction(
            transportService,
            mock(ActionFilters.class),
            securityContext,
            anonymousUser
        );

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<AuthenticateResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), new AuthenticateRequest(), new ActionListener<AuthenticateResponse>() {
            @Override
            public void onResponse(AuthenticateResponse authenticateResponse) {
                responseRef.set(authenticateResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), notNullValue());
        if (anonymousUser.enabled()) {
            final Authentication auth = responseRef.get().authentication();
            final User authUser = auth.getUser();
            List.of(authUser.roles()).containsAll(List.of(authentication.getUser().roles()));
            List.of(authUser.roles()).containsAll(List.of(anonymousUser.roles()));
            assertThat(authUser.authenticatedUser(), sameInstance(user.authenticatedUser()));
            assertThat(auth.getAuthenticatedBy(), sameInstance(auth.getAuthenticatedBy()));
            assertThat(auth.getLookedUpBy(), sameInstance(auth.getLookedUpBy()));
            assertThat(auth.getVersion(), sameInstance(auth.getVersion()));
            assertThat(auth.getAuthenticationType(), sameInstance(auth.getAuthenticationType()));
            assertThat(auth.getMetadata(), sameInstance(auth.getMetadata()));
        } else {
            assertThat(responseRef.get().authentication(), sameInstance(authentication));
        }
        assertThat(throwableRef.get(), nullValue());
    }

    private AnonymousUser prepareAnonymousUser() {
        final AnonymousUser anonymousUser = mock(AnonymousUser.class);
        if (randomBoolean()) {
            when(anonymousUser.enabled()).thenReturn(true);
            when(anonymousUser.roles()).thenReturn(randomList(1, 4, () -> randomAlphaOfLengthBetween(4, 12)).toArray(new String[0]));
        } else {
            when(anonymousUser.enabled()).thenReturn(false);
        }
        return anonymousUser;
    }

}
