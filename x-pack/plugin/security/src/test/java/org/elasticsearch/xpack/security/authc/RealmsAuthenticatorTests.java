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
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.util.List;
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
import static org.mockito.Mockito.spy;
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
    private RealmsAuthenticator realmsAuthenticator;

    @SuppressWarnings("unchecked")
    @Before
    public void init() throws Exception {
        Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test_node_name").build();
        threadContext = new ThreadContext(settings);
        RealmConfig realmConfig = mock(RealmConfig.class);
        when(realmConfig.settings()).thenReturn(settings);

        RealmConfig realmConfig1 = mock(RealmConfig.class);
        when(realmConfig1.settings()).thenReturn(settings);
        when(realmConfig1.name()).thenReturn("realm_name_1");
        when(realmConfig1.type()).thenReturn("realm_type_1");
        when(realmConfig1.domain()).thenReturn(randomFrom("domain", null));
        realm1 = spy(new TestRealm(realmConfig1));

        RealmConfig realmConfig2 = mock(RealmConfig.class);
        when(realmConfig2.settings()).thenReturn(settings);
        when(realmConfig2.name()).thenReturn("realm_name_2");
        when(realmConfig2.type()).thenReturn("realm_type_2");
        when(realmConfig2.domain()).thenReturn(randomFrom("domain", null));
        realm2 = spy(new TestRealm(realmConfig2));

        RealmConfig realmConfig3 = mock(RealmConfig.class);
        when(realmConfig3.settings()).thenReturn(settings);
        when(realmConfig3.name()).thenReturn("realm_name_3");
        when(realmConfig3.type()).thenReturn("realm_type_3");
        when(realmConfig3.domain()).thenReturn(randomFrom("domain", null));
        realm3 = spy(new TestRealm(realmConfig3));

        realms = mock(Realms.class);

        when(realms.getActiveRealms()).thenReturn(List.of(realm1, realm2));
        when(realms.getUnlicensedRealms()).thenReturn(List.of(realm3));

        request = randomBoolean()
            ? mock(AuthenticationService.AuditableRestRequest.class)
            : mock(AuthenticationService.AuditableTransportRequest.class);
        authenticationToken = mock(AuthenticationToken.class);
        username = randomAlphaOfLength(5);
        when(authenticationToken.principal()).thenReturn(username);
        user = new User(username);

        numInvalidation = new AtomicLong();
        lastSuccessfulAuthCache = mock(Cache.class);
        realmsAuthenticator = new RealmsAuthenticator(numInvalidation, lastSuccessfulAuthCache);
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
            final ActionListener<AuthenticationResult<User>> listener = (ActionListener<AuthenticationResult<User>>) invocationOnMock
                .getArguments()[1];
            listener.onResponse(AuthenticationResult.success(user));
            return null;
        }).when(successfulRealm).authenticate(eq(authenticationToken), any());

        when(unsuccessfulRealm.supports(authenticationToken)).thenReturn(randomBoolean());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult<User>> listener = (ActionListener<AuthenticationResult<User>>) invocationOnMock
                .getArguments()[1];
            listener.onResponse(AuthenticationResult.unsuccessful("unsuccessful", null));
            return null;
        }).when(unsuccessfulRealm).authenticate(eq(authenticationToken), any());

        final Authenticator.Context context = createAuthenticatorContext();
        context.addAuthenticationToken(authenticationToken);
        final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
        realmsAuthenticator.authenticate(context, future);
        final AuthenticationResult<Authentication> result = future.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        final Authentication authentication = result.getValue();
        assertThat(authentication.getUser(), is(user));
        assertThat(
            authentication.getAuthenticatedBy(),
            equalTo(new Authentication.RealmRef(successfulRealm.name(), successfulRealm.type(), "test_node_name", successfulRealm.domain()))
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
            final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
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
            new Authentication.RealmRef(authRealm.name(), authRealm.type(), "test_node_name", authRealm.domain()),
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
            new Authentication.RealmRef(authRealm.name(), authRealm.type(), "test_node_name", authRealm.domain()),
            null
        );
        final PlainActionFuture<Tuple<User, Authentication.RealmRef>> future = new PlainActionFuture<>();
        final ElasticsearchSecurityException e = new ElasticsearchSecurityException("fail");
        when(request.runAsDenied(any(), any())).thenReturn(e);
        realmsAuthenticator.lookupRunAsUser(createAuthenticatorContext(), authentication, future);
        assertThat(expectThrows(ElasticsearchSecurityException.class, future::actionGet), is(e));
    }

    private void configureRealmAuthResponse(Realm realm, AuthenticationResult<User> authenticationResult) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult<User>> listener = (ActionListener<AuthenticationResult<User>>) invocationOnMock
                .getArguments()[1];
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

    private static class TestRealm extends Realm {

        TestRealm(RealmConfig realmConfig) {
            super(realmConfig);
        }

        @Override
        public boolean supports(AuthenticationToken token) {
            return false;
        }

        @Override
        public AuthenticationToken token(ThreadContext context) {
            return null;
        }

        @Override
        public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult<User>> listener) {
            listener.onFailure(new Exception("not implemented"));
        }

        @Override
        public void lookupUser(String username, ActionListener<User> listener) {
            listener.onFailure(new Exception("not implemented"));
        }
    }
}
