/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyCredentials;
import org.elasticsearch.xpack.security.authc.AuthenticationService.AuditableRequest;
import org.elasticsearch.xpack.security.metric.SecurityMetricType;

import java.util.Map;
import java.util.function.LongSupplier;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApiKeyAuthenticatorTests extends AbstractAuthenticatorTests {

    public void testAuditingOnAuthenticationTermination() {
        final ApiKeyService apiKeyService = mock(ApiKeyService.class);
        final ApiKeyAuthenticator apiKeyAuthenticator = new ApiKeyAuthenticator(
            apiKeyService,
            randomAlphaOfLengthBetween(3, 8),
            MeterRegistry.NOOP
        );

        final Authenticator.Context context = mock(Authenticator.Context.class);

        final ApiKeyCredentials apiKeyCredentials = randomApiKeyCredentials();
        when(context.getMostRecentAuthenticationToken()).thenReturn(apiKeyCredentials);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(context.getThreadContext()).thenReturn(threadContext);
        final AuditableRequest auditableRequest = mock(AuditableRequest.class);
        when(context.getRequest()).thenReturn(auditableRequest);

        final Exception terminationError = randomFrom(new ElasticsearchException("termination error"), null);
        doAnswer(invocation -> {
            final ActionListener<AuthenticationResult<User>> listener = invocation.getArgument(2);
            listener.onResponse(AuthenticationResult.terminate("terminated by ApiKeyService", terminationError));
            return null;
        }).when(apiKeyService).tryAuthenticate(same(threadContext), same(apiKeyCredentials), anyActionListener());

        final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
        apiKeyAuthenticator.authenticate(context, future);

        final Exception e = expectThrows(Exception.class, future::actionGet);
        verify(auditableRequest).exceptionProcessingRequest(any(Exception.class), same(apiKeyCredentials));
        if (terminationError == null) {
            assertThat(e, instanceOf(ElasticsearchSecurityException.class));
            assertThat(e.getMessage(), containsString("terminated by ApiKeyService"));
        } else {
            assertThat(e, sameInstance(terminationError));
        }
    }

    public void testRecordingSuccessfulAuthenticationMetrics() {
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        final long initialNanoTime = randomLongBetween(0, 100);
        final TestNanoTimeSupplier nanoTimeSupplier = new TestNanoTimeSupplier(initialNanoTime);
        final ApiKeyService apiKeyService = mock(ApiKeyService.class);
        final ApiKeyAuthenticator apiKeyAuthenticator = createApiKeyAuthenticator(apiKeyService, telemetryPlugin, nanoTimeSupplier);

        final ApiKeyCredentials apiKeyCredentials = randomApiKeyCredentials();
        final Authenticator.Context context = mockApiKeyAuthenticationContext(apiKeyCredentials);

        final long executionTimeInNanos = randomLongBetween(0, 500);
        doAnswer(invocation -> {
            final ActionListener<AuthenticationResult<User>> listener = invocation.getArgument(2);
            nanoTimeSupplier.advanceTime(executionTimeInNanos);
            listener.onResponse(
                AuthenticationResult.success(
                    new User(randomAlphaOfLengthBetween(3, 8)),
                    Map.ofEntries(
                        Map.entry(AuthenticationField.API_KEY_ID_KEY, apiKeyCredentials.getId()),
                        Map.entry(AuthenticationField.API_KEY_TYPE_KEY, apiKeyCredentials.getExpectedType().value())
                    )
                )
            );
            return null;
        }).when(apiKeyService).tryAuthenticate(any(), same(apiKeyCredentials), anyActionListener());

        final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
        apiKeyAuthenticator.authenticate(context, future);
        final AuthenticationResult<Authentication> authResult = future.actionGet();
        assertThat(authResult.isAuthenticated(), equalTo(true));

        // verify that we always record a single authentication
        assertSingleSuccessAuthMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_API_KEY,
            Map.ofEntries(
                Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_ID, apiKeyCredentials.getId()),
                Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_TYPE, apiKeyCredentials.getExpectedType().value())
            )
        );

        // verify that there were no failures recorded
        assertZeroFailedAuthMetrics(telemetryPlugin, SecurityMetricType.AUTHC_API_KEY);

        // verify we recorded authentication time
        assertAuthenticationTimeMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_API_KEY,
            executionTimeInNanos,
            Map.ofEntries(
                Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_ID, apiKeyCredentials.getId()),
                Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_TYPE, apiKeyCredentials.getExpectedType().value())
            )
        );
    }

    public void testRecordingFailedAuthenticationMetrics() {
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        final long initialNanoTime = randomLongBetween(1, 100);
        final TestNanoTimeSupplier nanoTimeSupplier = new TestNanoTimeSupplier(initialNanoTime);
        final ApiKeyService apiKeyService = mock(ApiKeyService.class);
        final ApiKeyAuthenticator apiKeyAuthenticator = createApiKeyAuthenticator(apiKeyService, telemetryPlugin, nanoTimeSupplier);

        final ApiKeyCredentials apiKeyCredentials = randomApiKeyCredentials();
        final Authenticator.Context context = mockApiKeyAuthenticationContext(apiKeyCredentials);

        final Exception exception = randomFrom(new ElasticsearchException("API key auth exception"), null);
        final boolean failWithTermination = randomBoolean();
        final AuthenticationResult<User> failedAuth;
        if (failWithTermination) {
            failedAuth = AuthenticationResult.terminate("terminated API key auth", exception);
        } else {
            failedAuth = AuthenticationResult.unsuccessful("unsuccessful API key auth", exception);
        }

        final long executionTimeInNanos = randomLongBetween(0, 500);
        doAnswer(invocation -> {
            nanoTimeSupplier.advanceTime(executionTimeInNanos);
            final ActionListener<AuthenticationResult<User>> listener = invocation.getArgument(2);
            listener.onResponse(failedAuth);
            return Void.TYPE;
        }).when(apiKeyService).tryAuthenticate(any(), same(apiKeyCredentials), anyActionListener());
        final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
        apiKeyAuthenticator.authenticate(context, future);

        if (failWithTermination) {
            final Exception e = expectThrows(Exception.class, future::actionGet);
            if (exception == null) {
                assertThat(e, instanceOf(ElasticsearchSecurityException.class));
                assertThat(e.getMessage(), containsString("terminated API key auth"));
            } else {
                assertThat(e, sameInstance(exception));
            }
            assertSingleFailedAuthMetric(
                telemetryPlugin,
                SecurityMetricType.AUTHC_API_KEY,
                Map.ofEntries(
                    Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_ID, apiKeyCredentials.getId()),
                    Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_TYPE, apiKeyCredentials.getExpectedType().value()),
                    Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_AUTHC_FAILURE_REASON, "terminated API key auth")
                )
            );
        } else {
            var authResult = future.actionGet();
            assertThat(authResult.isAuthenticated(), equalTo(false));
            assertSingleFailedAuthMetric(
                telemetryPlugin,
                SecurityMetricType.AUTHC_API_KEY,
                Map.ofEntries(
                    Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_ID, apiKeyCredentials.getId()),
                    Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_TYPE, apiKeyCredentials.getExpectedType().value()),
                    Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_AUTHC_FAILURE_REASON, "unsuccessful API key auth")
                )
            );
        }

        // verify that there were no successes recorded
        assertZeroSuccessAuthMetrics(telemetryPlugin, SecurityMetricType.AUTHC_API_KEY);

        // verify we recorded authentication time
        assertAuthenticationTimeMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_API_KEY,
            executionTimeInNanos,
            Map.ofEntries(
                Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_ID, apiKeyCredentials.getId()),
                Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_TYPE, apiKeyCredentials.getExpectedType().value())
            )
        );
    }

    public void testRecordingFailedAuthenticationMetricsOnExceptions() {
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        final long initialNanoTime = randomLongBetween(0, 100);
        final TestNanoTimeSupplier nanoTimeSupplier = new TestNanoTimeSupplier(initialNanoTime);
        final ApiKeyService apiKeyService = mock(ApiKeyService.class);
        final ApiKeyAuthenticator apiKeyAuthenticator = createApiKeyAuthenticator(apiKeyService, telemetryPlugin, nanoTimeSupplier);

        final ApiKeyCredentials apiKeyCredentials = randomApiKeyCredentials();
        final Authenticator.Context context = mockApiKeyAuthenticationContext(apiKeyCredentials);

        final ElasticsearchSecurityException exception = new ElasticsearchSecurityException("API key auth exception");
        when(context.getRequest().exceptionProcessingRequest(same(exception), any())).thenReturn(exception);

        final long executionTimeInNanos = randomLongBetween(0, 500);
        doAnswer(invocation -> {
            nanoTimeSupplier.advanceTime(executionTimeInNanos);
            final ActionListener<AuthenticationResult<User>> listener = invocation.getArgument(2);
            listener.onFailure(exception);
            return Void.TYPE;
        }).when(apiKeyService).tryAuthenticate(any(), same(apiKeyCredentials), anyActionListener());

        final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
        apiKeyAuthenticator.authenticate(context, future);

        var e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e, sameInstance(exception));

        // expecting single recorded auth failure with message same as the thrown exception
        assertSingleFailedAuthMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_API_KEY,
            Map.ofEntries(
                Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_ID, apiKeyCredentials.getId()),
                Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_TYPE, apiKeyCredentials.getExpectedType().value()),
                Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_AUTHC_FAILURE_REASON, "API key auth exception")
            )
        );

        // verify that there were no successes recorded
        assertZeroSuccessAuthMetrics(telemetryPlugin, SecurityMetricType.AUTHC_API_KEY);

        // verify we recorded authentication time
        assertAuthenticationTimeMetric(
            telemetryPlugin,
            SecurityMetricType.AUTHC_API_KEY,
            executionTimeInNanos,
            Map.ofEntries(
                Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_ID, apiKeyCredentials.getId()),
                Map.entry(ApiKeyAuthenticator.ATTRIBUTE_API_KEY_TYPE, apiKeyCredentials.getExpectedType().value())
            )
        );
    }

    private static ApiKeyCredentials randomApiKeyCredentials() {
        return new ApiKeyCredentials(
            randomAlphaOfLength(12),
            new SecureString(randomAlphaOfLength(20).toCharArray()),
            randomFrom(ApiKey.Type.values())
        );
    }

    private static ApiKeyAuthenticator createApiKeyAuthenticator(
        ApiKeyService apiKeyService,
        TestTelemetryPlugin telemetryPlugin,
        LongSupplier nanoTimeSupplier
    ) {
        return new ApiKeyAuthenticator(
            apiKeyService,
            randomAlphaOfLengthBetween(3, 8),
            telemetryPlugin.getTelemetryProvider(Settings.EMPTY).getMeterRegistry(),
            nanoTimeSupplier
        );
    }

    private static Authenticator.Context mockApiKeyAuthenticationContext(ApiKeyCredentials apiKeyCredentials) {
        final Authenticator.Context context = mock(Authenticator.Context.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(context.getMostRecentAuthenticationToken()).thenReturn(apiKeyCredentials);
        when(context.getThreadContext()).thenReturn(threadContext);
        final AuditableRequest auditableRequest = mock(AuditableRequest.class);
        when(context.getRequest()).thenReturn(auditableRequest);
        return context;
    }

}
