/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountToken;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings.TOKEN_NAME_FIELD;
import static org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings.TOKEN_SOURCE_FIELD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServiceAccountAuthenticatorTests extends ESTestCase {

    public void testRecordingSuccessfulAuthenticationMetrics() {
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        final ServiceAccountService serviceAccountService = mock(ServiceAccountService.class);
        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        final ServiceAccountAuthenticator serviceAccountAuthenticator = new ServiceAccountAuthenticator(
            serviceAccountService,
            nodeName,
            telemetryPlugin.getTelemetryProvider(Settings.EMPTY).getMeterRegistry()
        );

        final ServiceAccount.ServiceAccountId accountId = new ServiceAccount.ServiceAccountId(
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8)
        );
        final String tokenName = randomAlphaOfLengthBetween(3, 8);
        final String tokenSource = randomFrom(TokenInfo.TokenSource.values()).name().toLowerCase(Locale.ROOT);
        final ServiceAccountToken serviceAccountToken = ServiceAccountToken.newToken(accountId, tokenName);

        final Authenticator.Context context = mockServiceAccountAuthenticatorContext(serviceAccountToken);

        doAnswer(invocation -> {
            final ActionListener<Authentication> listener = invocation.getArgument(2);
            Authentication authentication = Authentication.newServiceAccountAuthentication(
                new User(accountId.asPrincipal()),
                nodeName,
                Map.of(TOKEN_NAME_FIELD, serviceAccountToken.getTokenName(), TOKEN_SOURCE_FIELD, tokenSource)
            );
            listener.onResponse(authentication);
            return Void.TYPE;
        }).when(serviceAccountService).authenticateToken(same(serviceAccountToken), same(nodeName), anyActionListener());

        // Randomly call authentication multiple times
        final int numOfAuthentications = randomInt(3);
        for (int i = 0; i < numOfAuthentications; i++) {
            final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
            serviceAccountAuthenticator.authenticate(context, future);
            var authResult = future.actionGet();
            assertThat(authResult.isAuthenticated(), equalTo(true));
        }
        List<Measurement> successMetrics = telemetryPlugin.getLongCounterMeasurement(ServiceAccountAuthenticator.METRIC_SUCCESS_COUNT);
        assertThat(successMetrics.size(), equalTo(numOfAuthentications));

        successMetrics.forEach(metric -> {
            // verify that we always record a single authentication
            assertThat(metric.getLong(), equalTo(1L));

            // and that all attributes are present
            assertThat(
                metric.attributes(),
                equalTo(
                    Map.ofEntries(
                        Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_SERVICE_ACCOUNT_ID, accountId.asPrincipal()),
                        Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_SERVICE_ACCOUNT_TOKEN_NAME, tokenName),
                        Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_SERVICE_ACCOUNT_TOKEN_SOURCE, tokenSource)
                    )
                )
            );
        });

        // verify that there were no failures recorded
        assertZeroFailedAuthMetrics(telemetryPlugin);
    }

    public void testRecordingFailedAuthenticationMetrics() {
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        final ServiceAccountService serviceAccountService = mock(ServiceAccountService.class);
        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        final ServiceAccountAuthenticator serviceAccountAuthenticator = new ServiceAccountAuthenticator(
            serviceAccountService,
            nodeName,
            telemetryPlugin.getTelemetryProvider(Settings.EMPTY).getMeterRegistry()
        );

        final ServiceAccount.ServiceAccountId accountId = new ServiceAccount.ServiceAccountId(
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8)
        );
        final String tokenName = randomAlphaOfLengthBetween(3, 8);
        final String tokenSource = randomFrom(TokenInfo.TokenSource.values()).name().toLowerCase(Locale.ROOT);
        final ServiceAccountToken serviceAccountToken = ServiceAccountToken.newToken(accountId, tokenName);

        final Authenticator.Context context = mockServiceAccountAuthenticatorContext(serviceAccountToken);
        var failureError = new ElasticsearchSecurityException("failed to authenticate test service account", RestStatus.UNAUTHORIZED);
        when(context.getRequest().exceptionProcessingRequest(same(failureError), any())).thenReturn(failureError);

        doAnswer(invocation -> {
            final ActionListener<Authentication> listener = invocation.getArgument(2);
            listener.onFailure(failureError);
            return Void.TYPE;
        }).when(serviceAccountService).authenticateToken(same(serviceAccountToken), same(nodeName), anyActionListener());

        final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
        serviceAccountAuthenticator.authenticate(context, future);
        var e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e, sameInstance(failureError));

        List<Measurement> failuresMetrics = telemetryPlugin.getLongCounterMeasurement(ServiceAccountAuthenticator.METRIC_FAILURES_COUNT);
        assertThat(failuresMetrics.size(), equalTo(1));
        assertThat(
            failuresMetrics.get(0).attributes(),
            equalTo(
                Map.ofEntries(
                    Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_SERVICE_ACCOUNT_ID, accountId.asPrincipal()),
                    Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_SERVICE_ACCOUNT_TOKEN_NAME, tokenName),
                    Map.entry(ServiceAccountAuthenticator.ATTRIBUTE_AUTHC_FAILURE_REASON, "failed to authenticate test service account")
                )
            )
        );

        // verify that there were no successes recorded
        assertZeroSuccessAuthMetrics(telemetryPlugin);
    }

    private void assertZeroSuccessAuthMetrics(TestTelemetryPlugin telemetryPlugin) {
        List<Measurement> successMetrics = telemetryPlugin.getLongCounterMeasurement(ServiceAccountAuthenticator.METRIC_SUCCESS_COUNT);
        assertThat(successMetrics.size(), equalTo(0));
    }

    private void assertZeroFailedAuthMetrics(TestTelemetryPlugin telemetryPlugin) {
        List<Measurement> failuresMetrics = telemetryPlugin.getLongCounterMeasurement(ServiceAccountAuthenticator.METRIC_FAILURES_COUNT);
        assertThat(failuresMetrics.size(), equalTo(0));
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
