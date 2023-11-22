/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.BearerToken;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.Authenticator;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.RealmsAuthenticator;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JwtTokenExtractionTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testRealmLetsThroughInvalidJWTs() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        JwtRealm jwtRealm1 = mock(JwtRealm.class);
        doCallRealMethod().when(jwtRealm1).token(any());
        JwtRealm jwtRealm2 = mock(JwtRealm.class);
        doCallRealMethod().when(jwtRealm2).token(any());
        // mock realm extracts a (bearer) token from anything
        Realm mockRealm = mock(Realm.class);
        doAnswer(invocationOnMock -> {
            final ThreadContext threadContext1 = (ThreadContext) invocationOnMock.getArguments()[0];
            return new BearerToken(
                new SecureString("extracted by mock realm: " + threadContext1.getHeader(JwtRealm.HEADER_END_USER_AUTHENTICATION))
            );
        }).when(mockRealm).token(any());
        Realms realms = mock(Realms.class);
        // mock realm sits in-between
        when(realms.getActiveRealms()).thenReturn(List.of(jwtRealm1, mockRealm, jwtRealm2));
        RealmsAuthenticator realmsAuthenticator = new RealmsAuthenticator(mock(AtomicLong.class), (Cache<String, Realm>) mock(Cache.class));
        final Authenticator.Context context = new Authenticator.Context(
            threadContext,
            mock(AuthenticationService.AuditableRequest.class),
            null,
            true,
            realms
        );
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader(
                JwtRealm.HEADER_END_USER_AUTHENTICATION,
                JwtRealm.HEADER_END_USER_AUTHENTICATION_SCHEME + " " + "invalid JWT"
            );
            if (randomBoolean()) {
                threadContext.putHeader(
                    JwtRealm.HEADER_CLIENT_AUTHENTICATION,
                    JwtRealmSettings.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME + " " + "some shared secret"
                );
            }
            AuthenticationToken authenticationToken = realmsAuthenticator.extractCredentials(context);
            verify(jwtRealm1).token(any());
            verify(mockRealm).token(any());
            verify(jwtRealm2, never()).token(any());
            assertThat(authenticationToken, instanceOf(BearerToken.class));
            assertThat(((BearerToken) authenticationToken).credentials().toString(), containsString("invalid JWT"));
            assertThat(((BearerToken) authenticationToken).credentials().toString(), containsString("extracted by mock realm"));
        }
    }

}
