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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.metric.SecurityMetricType;

import java.util.Map;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServiceAccountAuthenticatorTests extends AbstractAuthenticatorTests {

    public void testRecordingSuccessfulAuthenticationMetrics() {
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        final long initialNanoTime = randomLongBetween(0, 100);
        final TestNanoTimeSupplier nanoTimeSupplier = new TestNanoTimeSupplier(initialNanoTime);
        final ServiceAccountService serviceAccountService = mock(ServiceAccountService.class);
        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        final ServiceAccountAuthenticator serviceAccountAuthenticator = new ServiceAccountAuthenticator(
            serviceAccountService,
            nodeName,
            telemetryPlugin.getTelemetryProvider(Settings.EMPTY).getMeterRegistry(),
            nanoTimeSupplier
        );

        final ServiceAccountToken serviceAccountToken = randomServiceAccountToken();
        final Authenticator.Context context = mockServiceAccountAuthenticatorContext(serviceAccountToken);

        final long executionTimeInNanos = randomLongBetween(0, 500);
        doAnswer(invocation -> {
            nanoTimeSupplier.advanceTime(executionTimeInNanos);
            final ActionListener<Authentication> listener = invocation.getArgument(2);
            Authentication authentication = Authentication.newServiceAccountAuthentication(
                new User(serviceAccountToken.getAccountId().asPrincipal()),
                nodeName,
                Map.of()
            );
            listener.onResponse(authentication);
            return Void.TYPE;
        }).when(serviceAccountService).authenticateToken(same(serviceAccountToken), same(nodeName), anyActionListener());

        final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
        serviceAccountAuthenticator.authenticate(context, future);
        var authResult = future.actionGet();
        assertThat(authResult.isAuthenticated(), equalTo(true));

        // verify we recorded success metric
        assertSingleSuccessAuthMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_SERVICE_ACCOUNT,
            Map.ofEntries(
                Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_SERVICE_ACCOUNT_ID, serviceAccountToken.getAccountId().asPrincipal()),
                Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_SERVICE_ACCOUNT_TOKEN_NAME, serviceAccountToken.getTokenName())
            )
        );

        // verify that there were no failures recorded
        assertZeroFailedAuthMetrics(telemetryPlugin, SecurityMetricType.AUTHC_SERVICE_ACCOUNT);

        // verify we recorded authentication time
        assertAuthenticationTimeMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_SERVICE_ACCOUNT,
            executionTimeInNanos,
            Map.ofEntries(
                Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_SERVICE_ACCOUNT_ID, serviceAccountToken.getAccountId().asPrincipal()),
                Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_SERVICE_ACCOUNT_TOKEN_NAME, serviceAccountToken.getTokenName())
            )
        );
    }

    public void testRecordingFailedAuthenticationMetrics() {
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        final long initialNanoTime = randomLongBetween(0, 100);
        final TestNanoTimeSupplier nanoTimeSupplier = new TestNanoTimeSupplier(initialNanoTime);
        final ServiceAccountService serviceAccountService = mock(ServiceAccountService.class);
        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        final ServiceAccountAuthenticator serviceAccountAuthenticator = new ServiceAccountAuthenticator(
            serviceAccountService,
            nodeName,
            telemetryPlugin.getTelemetryProvider(Settings.EMPTY).getMeterRegistry(),
            nanoTimeSupplier
        );

        final ServiceAccountToken serviceAccountToken = randomServiceAccountToken();
        final Authenticator.Context context = mockServiceAccountAuthenticatorContext(serviceAccountToken);
        var failureError = new ElasticsearchSecurityException("failed to authenticate test service account", RestStatus.UNAUTHORIZED);
        when(context.getRequest().exceptionProcessingRequest(same(failureError), any())).thenReturn(failureError);

        final long executionTimeInNanos = randomLongBetween(0, 500);
        doAnswer(invocation -> {
            nanoTimeSupplier.advanceTime(executionTimeInNanos);
            final ActionListener<Authentication> listener = invocation.getArgument(2);
            listener.onFailure(failureError);
            return Void.TYPE;
        }).when(serviceAccountService).authenticateToken(same(serviceAccountToken), same(nodeName), anyActionListener());

        final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
        serviceAccountAuthenticator.authenticate(context, future);
        var e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e, sameInstance(failureError));

        // verify we recorded failure metric
        assertSingleFailedAuthMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_SERVICE_ACCOUNT,
            Map.ofEntries(
                Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_SERVICE_ACCOUNT_ID, serviceAccountToken.getAccountId().asPrincipal()),
                Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_SERVICE_ACCOUNT_TOKEN_NAME, serviceAccountToken.getTokenName()),
                Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_AUTHC_FAILURE_REASON, "failed to authenticate test service account")
            )
        );

        // verify that there were no successes recorded
        assertZeroSuccessAuthMetrics(telemetryPlugin, SecurityMetricType.AUTHC_SERVICE_ACCOUNT);

        // verify we recorded authentication time
        assertAuthenticationTimeMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_SERVICE_ACCOUNT,
            executionTimeInNanos,
            Map.ofEntries(
                Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_SERVICE_ACCOUNT_ID, serviceAccountToken.getAccountId().asPrincipal()),
                Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_SERVICE_ACCOUNT_TOKEN_NAME, serviceAccountToken.getTokenName())
            )
        );
    }

    private static ServiceAccountToken randomServiceAccountToken() {
        return ServiceAccountToken.newToken(
            new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8)),
            randomAlphaOfLengthBetween(3, 8)
        );
    }

    private Authenticator.Context mockServiceAccountAuthenticatorContext(ServiceAccountToken token) {
        final Authenticator.Context context = mock(Authenticator.Context.class);
        when(context.getMostRecentAuthenticationToken()).thenReturn(token);
        when(context.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        when(context.getRequest()).thenReturn(auditableRequest);
        return context;
    }

}
