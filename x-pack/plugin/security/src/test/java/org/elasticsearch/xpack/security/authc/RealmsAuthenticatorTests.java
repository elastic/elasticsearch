/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RealmsAuthenticatorTests extends ESTestCase {

    private ThreadContext threadContext;
    private Realms realms;
    private Realm realm1;
    private Realm realm2;
    private Realm realm3;
    private AuthenticationService.AuditableRequest request;
    private AuthenticationToken authenticationToken;
    private String username;
    private User user;
    private AtomicLong numInvalidation;
    private Cache<String, Realm> lastSuccessfulAuthCache;
    private String nodeName;
    private RealmsAuthenticator realmsAuthenticator;

    @SuppressWarnings("unchecked")
    @Before
    public void init() throws Exception {
        threadContext = new ThreadContext(Settings.EMPTY);

        realms = mock(Realms.class);
        realm1 = mock(Realm.class);
        when(realm1.name()).thenReturn("realm1");
        when(realm1.type()).thenReturn("realm1");
        when(realm1.toString()).thenReturn("realm1/realm1");
        realm2 = mock(Realm.class);
        when(realm2.name()).thenReturn("realm2");
        when(realm2.type()).thenReturn("realm2");
        when(realm2.toString()).thenReturn("realm2/realm2");
        realm3 = mock(Realm.class);
        when(realm3.toString()).thenReturn("realm3/realm3");
        when(realms.getActiveRealms()).thenReturn(org.elasticsearch.core.List.of(realm1, realm2));
        when(realms.getUnlicensedRealms()).thenReturn(org.elasticsearch.core.List.of(realm3));

        request = randomBoolean()
            ? mock(AuthenticationService.AuditableHttpRequest.class)
            : mock(AuthenticationService.AuditableTransportRequest.class);
        authenticationToken = mock(AuthenticationToken.class);
        username = randomAlphaOfLength(5);
        when(authenticationToken.principal()).thenReturn(username);
        user = new User(username);

        nodeName = randomAlphaOfLength(8);
        numInvalidation = new AtomicLong();
        lastSuccessfulAuthCache = mock(Cache.class);
        realmsAuthenticator = new RealmsAuthenticator(nodeName, numInvalidation, lastSuccessfulAuthCache);
    }

    public void testExtractCredentials() {
        if (randomBoolean()) {
            when(realm1.token(threadContext)).thenReturn(authenticationToken);
        } else {
            when(realm2.token(threadContext)).thenReturn(authenticationToken);
        }
        assertThat(realmsAuthenticator.extractCredentials(createAuthenticatorContext()), is(authenticationToken));
    }

    public void testWillAuditOnCredentialsExtractionFailure() {
        final RuntimeException cause = new RuntimeException("fail");
        final ElasticsearchSecurityException wrapped = new ElasticsearchSecurityException("wrapped");
        when(request.exceptionProcessingRequest(cause, null)).thenReturn(wrapped);
        doThrow(cause).when(randomBoolean() ? realm1 : realm2).token(threadContext);
        assertThat(
            expectThrows(ElasticsearchSecurityException.class, () -> realmsAuthenticator.extractCredentials(createAuthenticatorContext())),
            is(wrapped)
        );
    }

    public void testAuthenticate() {
        when(lastSuccessfulAuthCache.get(username)).thenReturn(randomFrom(realm1, realm2, null));

        final Realm successfulRealm, unsuccessfulRealm;
        if (randomBoolean()) {
            successfulRealm = realm1;
            unsuccessfulRealm = realm2;
        } else {
            successfulRealm = realm2;
            unsuccessfulRealm = realm1;
        }

        when(successfulRealm.supports(authenticationToken)).thenReturn(true);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult> listener = (ActionListener<AuthenticationResult>) invocationOnMock.getArguments()[1];
            listener.onResponse(AuthenticationResult.success(user));
            return null;
        }).when(successfulRealm).authenticate(eq(authenticationToken), any());

        when(unsuccessfulRealm.supports(authenticationToken)).thenReturn(randomBoolean());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult> listener = (ActionListener<AuthenticationResult>) invocationOnMock.getArguments()[1];
            listener.onResponse(AuthenticationResult.unsuccessful("unsuccessful", null));
            return null;
        }).when(unsuccessfulRealm).authenticate(eq(authenticationToken), any());

        final Authenticator.Context context = createAuthenticatorContext();
        context.addAuthenticationToken(authenticationToken);
        final PlainActionFuture<Authenticator.Result> future = new PlainActionFuture<>();
        realmsAuthenticator.authenticate(context, future);
        final Authenticator.Result result = future.actionGet();
        assertThat(result.getStatus(), is(Authenticator.Status.SUCCESS));
        final Authentication authentication = result.getAuthentication();
        assertThat(authentication.getUser(), is(user));
        assertThat(
            authentication.getAuthenticatedBy(),
            equalTo(new Authentication.RealmRef(successfulRealm.name(), successfulRealm.type(), nodeName))
        );
    }

    public void testNullUser() throws IllegalAccessException {
        if (randomBoolean()) {
            when(realm1.supports(authenticationToken)).thenReturn(true);
            configureRealmAuthResponse(realm1, AuthenticationResult.unsuccessful("unsuccessful", null));
        } else {
            when(realm1.supports(authenticationToken)).thenReturn(false);
        }

        if (randomBoolean()) {
            when(realm2.supports(authenticationToken)).thenReturn(true);
            configureRealmAuthResponse(realm2, AuthenticationResult.unsuccessful("unsuccessful", null));
        } else {
            when(realm2.supports(authenticationToken)).thenReturn(false);
        }

        final Authenticator.Context context = createAuthenticatorContext();
        context.addAuthenticationToken(authenticationToken);
        final ElasticsearchSecurityException e = new ElasticsearchSecurityException("fail");
        when(request.authenticationFailed(authenticationToken)).thenReturn(e);

        final Logger unlicensedRealmsLogger = LogManager.getLogger(RealmsAuthenticator.class);
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        try {
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "unlicensed realms",
                    RealmsAuthenticator.class.getName(),
                    Level.WARN,
                    "Authentication failed using realms [realm1/realm1,realm2/reaml2]."
                        + " Realms [realm3/realm3] were skipped because they are not permitted on the current license"
                )
            );
            final PlainActionFuture<Authenticator.Result> future = new PlainActionFuture<>();
            realmsAuthenticator.authenticate(context, future);
            assertThat(expectThrows(ElasticsearchSecurityException.class, future::actionGet), is(e));
        } finally {
            Loggers.removeAppender(unlicensedRealmsLogger, mockAppender);
            mockAppender.stop();
        }
    }

    public void testLookupRunAsUser() {
        when(lastSuccessfulAuthCache.get(username)).thenReturn(randomFrom(realm1, realm2, null));
        final String runAsUsername = randomAlphaOfLength(10);
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, runAsUsername);
        final boolean lookupByRealm1 = randomBoolean();
        if (lookupByRealm1) {
            configureRealmUserResponse(realm1, runAsUsername);
            configureRealmUserResponse(realm2, null);
        } else {
            configureRealmUserResponse(realm1, null);
            configureRealmUserResponse(realm2, runAsUsername);
        }

        final Realm authRealm = randomFrom(realm1, realm2);
        final Authentication authentication = new Authentication(
            user,
            new Authentication.RealmRef(authRealm.name(), authRealm.type(), nodeName),
            null
        );
        final PlainActionFuture<Tuple<User, Authentication.RealmRef>> future = new PlainActionFuture<>();
        realmsAuthenticator.lookupRunAsUser(createAuthenticatorContext(), authentication, future);
        final Tuple<User, Authentication.RealmRef> tuple = future.actionGet();
        assertThat(tuple.v1(), equalTo(new User(runAsUsername)));
        assertThat(tuple.v2().getName(), is(lookupByRealm1 ? realm1.name() : realm2.name()));
    }

    public void testNullRunAsUser() {
        final PlainActionFuture<Tuple<User, Authentication.RealmRef>> future = new PlainActionFuture<>();
        realmsAuthenticator.lookupRunAsUser(createAuthenticatorContext(), mock(Authentication.class), future);
        assertThat(future.actionGet(), nullValue());
    }

    public void testEmptyRunAsUsernameWillFail() {
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "");
        final Realm authRealm = randomFrom(realm1, realm2);
        final Authentication authentication = new Authentication(
            user,
            new Authentication.RealmRef(authRealm.name(), authRealm.type(), nodeName),
            null
        );
        final PlainActionFuture<Tuple<User, Authentication.RealmRef>> future = new PlainActionFuture<>();
        final ElasticsearchSecurityException e = new ElasticsearchSecurityException("fail");
        when(request.runAsDenied(any(), any())).thenReturn(e);
        realmsAuthenticator.lookupRunAsUser(createAuthenticatorContext(), authentication, future);
        assertThat(expectThrows(ElasticsearchSecurityException.class, future::actionGet), is(e));
    }

    private void configureRealmAuthResponse(Realm realm, AuthenticationResult authenticationResult) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult> listener = (ActionListener<AuthenticationResult>) invocationOnMock.getArguments()[1];
            listener.onResponse(authenticationResult);
            return null;
        }).when(realm).authenticate(eq(authenticationToken), any());
    }

    private void configureRealmUserResponse(Realm realm, String runAsUsername) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<User> listener = (ActionListener<User>) invocationOnMock.getArguments()[1];
            listener.onResponse(runAsUsername == null ? null : new User(runAsUsername));
            return null;
        }).when(realm).lookupUser(runAsUsername == null ? anyString() : eq(runAsUsername), any());
    }

    private Authenticator.Context createAuthenticatorContext() {
        return new Authenticator.Context(threadContext, request, null, true, realms);
    }
}
