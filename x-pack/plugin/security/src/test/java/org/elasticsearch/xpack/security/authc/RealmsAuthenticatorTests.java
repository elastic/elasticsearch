/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.metric.SecurityMetricType;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RealmsAuthenticatorTests extends AbstractAuthenticatorTests {

    private ThreadContext threadContext;
    private Realms realms;
    private RealmDomain domain1;
    private Realm realm1;
    private RealmDomain domain2;
    private Realm realm2;
    private RealmDomain domain3;
    private Realm realm3;
    private AuthenticationService.AuditableRequest request;
    private AuthenticationToken authenticationToken;
    private String username;
    private User user;
    private AtomicLong numInvalidation;
    private Cache<String, Realm> lastSuccessfulAuthCache;
    private String nodeName;
    private RealmsAuthenticator realmsAuthenticator;
    private TestTelemetryPlugin telemetryPlugin;
    private TestNanoTimeSupplier nanoTimeSupplier;

    @SuppressWarnings("unchecked")
    @Before
    public void init() throws Exception {
        threadContext = new ThreadContext(Settings.EMPTY);
        nodeName = randomAlphaOfLength(8);

        realms = mock(Realms.class);

        domain1 = randomFrom(new RealmDomain("domain1", Set.of()), null);
        realm1 = mock(Realm.class);
        when(realm1.name()).thenReturn("realm1");
        when(realm1.type()).thenReturn("realm1");
        when(realm1.toString()).thenReturn("realm1/realm1");
        when(realm1.realmRef()).thenReturn(new Authentication.RealmRef("realm1", "realm1", nodeName, domain1));
        domain2 = randomFrom(new RealmDomain("domain2", Set.of()), null);
        realm2 = mock(Realm.class);
        when(realm2.name()).thenReturn("realm2");
        when(realm2.type()).thenReturn("realm2");
        when(realm2.toString()).thenReturn("realm2/realm2");
        when(realm2.realmRef()).thenReturn(new Authentication.RealmRef("realm2", "realm2", nodeName, domain2));
        domain3 = randomFrom(new RealmDomain("domain3", Set.of()), null);
        realm3 = mock(Realm.class);
        when(realm3.toString()).thenReturn("realm3/realm3");
        when(realms.getActiveRealms()).thenReturn(List.of(realm1, realm2));
        when(realms.getUnlicensedRealms()).thenReturn(List.of(realm3));
        when(realm3.realmRef()).thenReturn(new Authentication.RealmRef("realm3", "realm3", nodeName, domain3));

        request = randomBoolean()
            ? mock(AuthenticationService.AuditableHttpRequest.class)
            : mock(AuthenticationService.AuditableTransportRequest.class);
        authenticationToken = mock(AuthenticationToken.class);
        username = randomAlphaOfLength(5);
        when(authenticationToken.principal()).thenReturn(username);
        user = new User(username);

        numInvalidation = new AtomicLong();
        lastSuccessfulAuthCache = mock(Cache.class);
        telemetryPlugin = new TestTelemetryPlugin();
        nanoTimeSupplier = new TestNanoTimeSupplier(randomLongBetween(0, 100));
        realmsAuthenticator = new RealmsAuthenticator(
            numInvalidation,
            lastSuccessfulAuthCache,
            telemetryPlugin.getTelemetryProvider(Settings.EMPTY).getMeterRegistry(),
            nanoTimeSupplier
        );
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
        assertThat(authentication.getEffectiveSubject().getUser(), is(user));
        assertThat(
            authentication.getAuthenticatingSubject().getRealm(),
            is(
                new Authentication.RealmRef(
                    successfulRealm.name(),
                    successfulRealm.type(),
                    nodeName,
                    successfulRealm.realmRef().getDomain()
                )
            )
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

        try (var mockLog = MockLog.capture(RealmsAuthenticator.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "unlicensed realms",
                    RealmsAuthenticator.class.getName(),
                    Level.WARN,
                    "Authentication failed using realms [realm1/realm1,realm2/realm2]."
                        + " Realms [realm3/realm3] were skipped because they are not permitted on the current license"
                )
            );
            final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
            realmsAuthenticator.authenticate(context, future);
            assertThat(expectThrows(ElasticsearchSecurityException.class, future::actionGet), is(e));
            mockLog.assertAllExpectationsMatched();
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

        final Authentication authentication = Authentication.newRealmAuthentication(user, authRealm.realmRef());
        final PlainActionFuture<Tuple<User, Realm>> future = new PlainActionFuture<>();
        realmsAuthenticator.lookupRunAsUser(createAuthenticatorContext(), authentication, future);
        final Tuple<User, Realm> tuple = future.actionGet();
        assertThat(tuple.v1(), equalTo(new User(runAsUsername)));
        assertThat(tuple.v2().name(), is(lookupByRealm1 ? realm1.name() : realm2.name()));
        assertThat(tuple.v2().realmRef(), is(lookupByRealm1 ? realm1.realmRef() : realm2.realmRef()));
    }

    public void testNullRunAsUser() {
        final PlainActionFuture<Tuple<User, Realm>> future = new PlainActionFuture<>();
        realmsAuthenticator.lookupRunAsUser(createAuthenticatorContext(), AuthenticationTestHelper.builder().build(false), future);
        assertThat(future.actionGet(), nullValue());
    }

    public void testEmptyRunAsUsernameWillFail() {
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "");
        final Realm authRealm = randomFrom(realm1, realm2);
        final Authentication authentication = Authentication.newRealmAuthentication(user, authRealm.realmRef());
        final PlainActionFuture<Tuple<User, Realm>> future = new PlainActionFuture<>();
        final ElasticsearchSecurityException e = new ElasticsearchSecurityException("fail");
        when(request.runAsDenied(any(), any())).thenReturn(e);
        realmsAuthenticator.lookupRunAsUser(createAuthenticatorContext(), authentication, future);
        assertThat(expectThrows(ElasticsearchSecurityException.class, future::actionGet), is(e));
    }

    public void testRecodingSuccessfulAuthenticationMetrics() {
        when(lastSuccessfulAuthCache.get(username)).thenReturn(randomFrom(realm1, realm2, null));
        final Realm successfulRealm = randomFrom(realm1, realm2);
        when(successfulRealm.supports(authenticationToken)).thenReturn(true);
        final long successfulExecutionTimeInNanos = randomLongBetween(0, 500);
        doAnswer(invocationOnMock -> {
            nanoTimeSupplier.advanceTime(successfulExecutionTimeInNanos);
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult<User>> listener = (ActionListener<AuthenticationResult<User>>) invocationOnMock
                .getArguments()[1];
            listener.onResponse(AuthenticationResult.success(user));
            return null;
        }).when(successfulRealm).authenticate(eq(authenticationToken), any());

        final Authenticator.Context context = createAuthenticatorContext();
        context.addAuthenticationToken(authenticationToken);

        final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
        realmsAuthenticator.authenticate(context, future);
        final AuthenticationResult<Authentication> result = future.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));

        assertSingleSuccessAuthMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_REALMS,
            Map.ofEntries(
                Map.entry(RealmsAuthenticator.ATTRIBUTE_REALM_NAME, successfulRealm.name()),
                Map.entry(RealmsAuthenticator.ATTRIBUTE_REALM_TYPE, successfulRealm.type())
            )
        );

        assertZeroFailedAuthMetrics(telemetryPlugin, SecurityMetricType.AUTHC_REALMS);

        assertAuthenticationTimeMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_REALMS,
            successfulExecutionTimeInNanos,
            Map.ofEntries(
                Map.entry(RealmsAuthenticator.ATTRIBUTE_REALM_NAME, successfulRealm.name()),
                Map.entry(RealmsAuthenticator.ATTRIBUTE_REALM_TYPE, successfulRealm.type())
            )
        );
    }

    public void testRecordingFailedAuthenticationMetric() {
        when(lastSuccessfulAuthCache.get(username)).thenReturn(randomFrom(realm1, realm2, null));

        final Realm unsuccessfulRealm;
        if (randomBoolean()) {
            when(realm1.supports(authenticationToken)).thenReturn(false);
            unsuccessfulRealm = realm2;
        } else {
            when(realm2.supports(authenticationToken)).thenReturn(false);
            unsuccessfulRealm = realm1;
        }

        when(unsuccessfulRealm.supports(authenticationToken)).thenReturn(true);
        final long unsuccessfulExecutionTimeInNanos = randomLongBetween(0, 500);
        doAnswer(invocationOnMock -> {
            nanoTimeSupplier.advanceTime(unsuccessfulExecutionTimeInNanos);
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult<User>> listener = (ActionListener<AuthenticationResult<User>>) invocationOnMock
                .getArguments()[1];
            listener.onResponse(AuthenticationResult.unsuccessful("unsuccessful realms authentication", null));
            return null;
        }).when(unsuccessfulRealm).authenticate(eq(authenticationToken), any());

        final Authenticator.Context context = createAuthenticatorContext();
        final ElasticsearchSecurityException exception = new ElasticsearchSecurityException("realms authentication failed");
        when(request.authenticationFailed(same(authenticationToken))).thenReturn(exception);
        context.addAuthenticationToken(authenticationToken);

        final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
        realmsAuthenticator.authenticate(context, future);
        var e = expectThrows(ElasticsearchSecurityException.class, () -> future.actionGet());
        assertThat(e, sameInstance(exception));

        assertSingleFailedAuthMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_REALMS,
            Map.ofEntries(
                Map.entry(RealmsAuthenticator.ATTRIBUTE_REALM_NAME, unsuccessfulRealm.name()),
                Map.entry(RealmsAuthenticator.ATTRIBUTE_REALM_TYPE, unsuccessfulRealm.type()),
                Map.entry(RealmsAuthenticator.ATTRIBUTE_REALM_AUTHC_FAILURE_REASON, "unsuccessful realms authentication")
            )
        );

        assertZeroSuccessAuthMetrics(telemetryPlugin, SecurityMetricType.AUTHC_REALMS);

        assertAuthenticationTimeMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_REALMS,
            unsuccessfulExecutionTimeInNanos,
            Map.ofEntries(
                Map.entry(RealmsAuthenticator.ATTRIBUTE_REALM_NAME, unsuccessfulRealm.name()),
                Map.entry(RealmsAuthenticator.ATTRIBUTE_REALM_TYPE, unsuccessfulRealm.type())
            )
        );

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
}
