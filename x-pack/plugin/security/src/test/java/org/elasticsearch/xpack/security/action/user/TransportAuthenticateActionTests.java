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
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportAuthenticateActionTests extends ESTestCase {

    private ThreadContext threadContent = new ThreadContext(Settings.EMPTY);

    public void testInternalUser() {
        SecurityContext securityContext = mock(SecurityContext.class);
        final Authentication authentication = AuthenticationTestHelper.builder().internal().build();
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
        action.doExecute(mock(Task.class), AuthenticateRequest.INSTANCE, new ActionListener<AuthenticateResponse>() {
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
        action.doExecute(mock(Task.class), AuthenticateRequest.INSTANCE, new ActionListener<AuthenticateResponse>() {
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
        final AnonymousUser anonymousUser = prepareAnonymousUser();
        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        final Authentication authentication = AuthenticationTestHelper.builder().user(user).build();
        final User effectiveUser = authentication.getEffectiveSubject().getUser();
        final boolean operator = randomBoolean();

        if (operator) {
            threadContent.putHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY, AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR);
        } else if (randomBoolean()) {
            threadContent.putHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY, AuthenticationField.PRIVILEGE_CATEGORY_VALUE_EMPTY);
        }

        TransportAuthenticateAction action = prepareAction(anonymousUser, effectiveUser, authentication);

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<AuthenticateResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), AuthenticateRequest.INSTANCE, new ActionListener<>() {
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
        assertThat(responseRef.get().isOperator(), is(operator));
        if (anonymousUser.enabled() && false == (authentication.isApiKey() || authentication.isCrossClusterAccess())) {
            // Roles of anonymousUser are added to non api key authentication
            final Authentication auth = responseRef.get().authentication();
            final User userInResponse = auth.getEffectiveSubject().getUser();
            assertThat(
                userInResponse.roles(),
                arrayContainingInAnyOrder(ArrayUtils.concat(authentication.getEffectiveSubject().getUser().roles(), anonymousUser.roles()))
            );
            assertThat(auth.isRunAs(), is(authentication.isRunAs()));
            if (auth.isRunAs()) {
                assertThat(auth.getAuthenticatingSubject().getUser(), sameInstance(authentication.getAuthenticatingSubject().getUser()));
            }
            assertThat(auth.getAuthenticatingSubject().getRealm(), sameInstance(authentication.getAuthenticatingSubject().getRealm()));
            assertThat(auth.getEffectiveSubject().getRealm(), sameInstance(authentication.getEffectiveSubject().getRealm()));
            assertThat(
                auth.getEffectiveSubject().getTransportVersion(),
                sameInstance(authentication.getEffectiveSubject().getTransportVersion())
            );
            assertThat(auth.getAuthenticationType(), sameInstance(authentication.getAuthenticationType()));
            assertThat(
                auth.getAuthenticatingSubject().getMetadata(),
                sameInstance(authentication.getAuthenticatingSubject().getMetadata())
            );
        } else {
            assertThat(responseRef.get().authentication(), sameInstance(authentication));
        }
        assertThat(throwableRef.get(), nullValue());
    }

    public void testShouldNotAddAnonymousRolesForApiKeyOrServiceAccount() {
        final AnonymousUser anonymousUser = prepareAnonymousUser();

        final Authentication authentication;

        if (randomBoolean()) {
            authentication = AuthenticationTestHelper.builder().apiKey().build();
        } else {
            authentication = AuthenticationTestHelper.builder().serviceAccount().build();
        }
        final User user = authentication.getEffectiveSubject().getUser();
        // API key or service account have no named roles
        assertThat(user.roles(), emptyArray());

        TransportAuthenticateAction action = prepareAction(anonymousUser, user, authentication);

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<AuthenticateResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), AuthenticateRequest.INSTANCE, new ActionListener<>() {
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
        // Anonymous roles should not be added which means there is no change to the authentication at all
        assertThat(responseRef.get().authentication(), sameInstance(authentication));
        assertThat(throwableRef.get(), nullValue());
    }

    private TransportAuthenticateAction prepareAction(AnonymousUser anonymousUser, User user, Authentication authentication) {
        SecurityContext securityContext = mock(SecurityContext.class);
        when(securityContext.getAuthentication()).thenReturn(authentication);
        when(securityContext.getUser()).thenReturn(user);
        when(securityContext.getThreadContext()).thenReturn(this.threadContent);

        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        return new TransportAuthenticateAction(transportService, mock(ActionFilters.class), securityContext, anonymousUser);
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
