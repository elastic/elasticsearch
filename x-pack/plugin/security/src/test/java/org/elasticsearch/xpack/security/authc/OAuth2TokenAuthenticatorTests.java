/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.support.BearerToken;
import org.elasticsearch.xpack.security.metric.SecurityMetricType;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OAuth2TokenAuthenticatorTests extends AbstractAuthenticatorTests {

    public void testRecordingSuccessfulAuthenticationMetrics() {
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        final long initialNanoTime = randomLongBetween(0, 100);
        final TestNanoTimeSupplier nanoTimeSupplier = new TestNanoTimeSupplier(initialNanoTime);
        final TokenService tokenService = mock(TokenService.class);
        final OAuth2TokenAuthenticator oauth2Authenticator = new OAuth2TokenAuthenticator(
            tokenService,
            telemetryPlugin.getTelemetryProvider(Settings.EMPTY).getMeterRegistry(),
            nanoTimeSupplier
        );

        final BearerToken bearerToken = randomBearerToken();
        final Authenticator.Context context = mockAuthenticatorContext(bearerToken);

        final long executionTimeInNanos = randomLongBetween(0, 500);
        doAnswer(invocation -> {
            nanoTimeSupplier.advanceTime(executionTimeInNanos);
            final ActionListener<UserToken> listener = invocation.getArgument(1);
            final Authentication authentication = AuthenticationTestHelper.builder()
                .user(AuthenticationTestHelper.randomUser())
                .realmRef(AuthenticationTestHelper.randomRealmRef())
                .build(false);
            final int seconds = randomIntBetween(0, Math.toIntExact(TimeValue.timeValueMinutes(30L).getSeconds()));
            final Instant expirationTime = Clock.systemUTC().instant().plusSeconds(seconds);
            final UserToken userToken = new UserToken(authentication, expirationTime);
            listener.onResponse(userToken);
            return Void.TYPE;
        }).when(tokenService).tryAuthenticateToken(any(SecureString.class), anyActionListener());

        final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
        oauth2Authenticator.authenticate(context, future);
        var authResult = future.actionGet();
        assertThat(authResult.isAuthenticated(), equalTo(true));

        // verify we recorded success metric
        assertSingleSuccessAuthMetric(telemetryPlugin, SecurityMetricType.AUTHC_OAUTH2_TOKEN, Map.of());

        // verify that there were no failures recorded
        assertZeroFailedAuthMetrics(telemetryPlugin, SecurityMetricType.AUTHC_OAUTH2_TOKEN);

        // verify we recorded authentication time
        assertAuthenticationTimeMetric(telemetryPlugin, SecurityMetricType.AUTHC_OAUTH2_TOKEN, executionTimeInNanos, Map.of());
    }

    public void testRecordingFailedAuthenticationMetrics() {
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        final long initialNanoTime = randomLongBetween(0, 100);
        final TestNanoTimeSupplier nanoTimeSupplier = new TestNanoTimeSupplier(initialNanoTime);
        final TokenService tokenService = mock(TokenService.class);
        final OAuth2TokenAuthenticator oauth2Authenticator = new OAuth2TokenAuthenticator(
            tokenService,
            telemetryPlugin.getTelemetryProvider(Settings.EMPTY).getMeterRegistry(),
            nanoTimeSupplier
        );

        final BearerToken bearerToken = randomBearerToken();
        final Authenticator.Context context = mockAuthenticatorContext(bearerToken);

        var failureError = new ElasticsearchSecurityException("failed to authenticate OAuth2 token", RestStatus.UNAUTHORIZED);
        when(context.getRequest().exceptionProcessingRequest(same(failureError), any())).thenReturn(failureError);

        final long executionTimeInNanos = randomLongBetween(0, 500);
        doAnswer(invocation -> {
            nanoTimeSupplier.advanceTime(executionTimeInNanos);
            final ActionListener<Authentication> listener = invocation.getArgument(1);
            listener.onFailure(failureError);
            return Void.TYPE;
        }).when(tokenService).tryAuthenticateToken(any(SecureString.class), anyActionListener());

        final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
        oauth2Authenticator.authenticate(context, future);
        var e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e, sameInstance(failureError));

        // verify we recorded failure metric
        assertSingleFailedAuthMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_OAUTH2_TOKEN,
            Map.ofEntries(Map.entry(OAuth2TokenAuthenticator.ATTRIBUTE_AUTHC_FAILURE_REASON, "failed to authenticate OAuth2 token"))
        );

        // verify that there were no successes recorded
        assertZeroSuccessAuthMetrics(telemetryPlugin, SecurityMetricType.AUTHC_OAUTH2_TOKEN);

        // verify we recorded authentication time
        assertAuthenticationTimeMetric(telemetryPlugin, SecurityMetricType.AUTHC_OAUTH2_TOKEN, executionTimeInNanos, Map.of());
    }

    private static BearerToken randomBearerToken() {
        return new BearerToken(new SecureString(randomAlphaOfLengthBetween(5, 10).toCharArray()));
    }

    private Authenticator.Context mockAuthenticatorContext(BearerToken token) {
        final Authenticator.Context context = mock(Authenticator.Context.class);
        when(context.getMostRecentAuthenticationToken()).thenReturn(token);
        when(context.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        when(context.getRequest()).thenReturn(auditableRequest);
        return context;
    }
}
