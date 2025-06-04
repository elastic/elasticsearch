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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authc.support.BearerToken;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyCredentials;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges.OperatorPrivilegesService;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AuthenticatorChainTests extends ESTestCase {

    private ThreadContext threadContext;
    private Realms realms;
    private OperatorPrivilegesService operatorPrivilegesService;
    private AnonymousUser anonymousUser;
    private AuthenticationContextSerializer authenticationContextSerializer;
    private ServiceAccountAuthenticator serviceAccountAuthenticator;
    private OAuth2TokenAuthenticator oAuth2TokenAuthenticator;
    private ApiKeyAuthenticator apiKeyAuthenticator;
    private RealmsAuthenticator realmsAuthenticator;
    private Authentication authentication;
    private User fallbackUser;
    private AuthenticatorChain authenticatorChain;

    @Before
    public void init() {
        final Settings settings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLength(8))
            .put(AuthenticationServiceField.RUN_AS_ENABLED.getKey(), "true")
            .put(AnonymousUser.USERNAME_SETTING.getKey(), "anonymous")
            .put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous_role")
            .build();
        threadContext = new ThreadContext(settings);
        realms = mock(Realms.class);
        operatorPrivilegesService = mock(OperatorPrivilegesService.class);
        anonymousUser = mock(AnonymousUser.class);
        when(anonymousUser.enabled()).thenReturn(true);

        authenticationContextSerializer = mock(AuthenticationContextSerializer.class);
        serviceAccountAuthenticator = mock(ServiceAccountAuthenticator.class);
        oAuth2TokenAuthenticator = mock(OAuth2TokenAuthenticator.class);
        apiKeyAuthenticator = mock(ApiKeyAuthenticator.class);
        realmsAuthenticator = mock(RealmsAuthenticator.class);
        when(realms.getActiveRealms()).thenReturn(List.of(mock(Realm.class)));
        when(realms.getUnlicensedRealms()).thenReturn(List.of());
        final User user = new User(randomAlphaOfLength(8));
        authentication = AuthenticationTestHelper.builder().user(user).build(false);
        fallbackUser = AuthenticationTestHelper.randomInternalUser();
        authenticatorChain = new AuthenticatorChain(
            settings,
            operatorPrivilegesService,
            anonymousUser,
            authenticationContextSerializer,
            serviceAccountAuthenticator,
            oAuth2TokenAuthenticator,
            apiKeyAuthenticator,
            realmsAuthenticator
        );
    }

    public void testAuthenticateWillLookForExistingAuthenticationFirst() throws IOException {
        final Authenticator.Context context = createAuthenticatorContext(mock(AuthenticationService.AuditableTransportRequest.class));
        when(authenticationContextSerializer.readFromContext(any())).thenReturn(authentication);

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticate(context, future);
        assertThat(future.actionGet(), is(authentication));
        verifyNoMoreInteractions(serviceAccountAuthenticator);
        verifyNoMoreInteractions(oAuth2TokenAuthenticator);
        verifyNoMoreInteractions(apiKeyAuthenticator);
        verifyNoMoreInteractions(realmsAuthenticator);
        verify(authenticationContextSerializer, never()).writeToContext(any(), any());
        verify(operatorPrivilegesService, times(1)).maybeMarkOperatorUser(eq(authentication), any());
    }

    public void testAuthenticateFailsIfExistingAuthenticationFoundForRestRequest() throws IOException {
        final AuthenticationService.AuditableHttpRequest auditableHttpRequest = mock(AuthenticationService.AuditableHttpRequest.class);
        final ElasticsearchSecurityException e = new ElasticsearchSecurityException("fail");
        when(auditableHttpRequest.tamperedRequest()).thenReturn(e);
        final Authenticator.Context context = createAuthenticatorContext(auditableHttpRequest);
        when(authenticationContextSerializer.readFromContext(any())).thenReturn(AuthenticationTestHelper.builder().build());

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticate(context, future);
        assertThat(expectThrows(ElasticsearchSecurityException.class, future::actionGet), is(e));
    }

    public void testAuthenticateWithServiceAccount() throws IOException {
        final boolean shouldExtractCredentials = randomBoolean();
        final Authenticator.Context context;
        if (shouldExtractCredentials) {
            context = createAuthenticatorContext();
            when(serviceAccountAuthenticator.extractCredentials(context)).thenReturn(mock(ServiceAccountToken.class));
        } else {
            context = createAuthenticatorContext(mock(ServiceAccountToken.class));
        }
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult<Authentication>> listener = (ActionListener<
                AuthenticationResult<Authentication>>) invocationOnMock.getArguments()[1];
            listener.onResponse(AuthenticationResult.success(authentication));
            return null;
        }).when(serviceAccountAuthenticator).authenticate(eq(context), any());
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticate(context, future);
        assertThat(future.actionGet(), is(authentication));
        if (shouldExtractCredentials) {
            verify(serviceAccountAuthenticator).extractCredentials(eq(context));
        } else {
            verify(serviceAccountAuthenticator, never()).extractCredentials(any());
        }
        verify(serviceAccountAuthenticator).authenticate(eq(context), any());
        verifyNoMoreInteractions(oAuth2TokenAuthenticator);
        verifyNoMoreInteractions(apiKeyAuthenticator);
        verifyNoMoreInteractions(realmsAuthenticator);
        verify(authenticationContextSerializer).writeToContext(eq(authentication), any());
        verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(authentication), any());
    }

    public void testAuthenticateWithOAuth2Token() throws IOException {
        final boolean shouldExtractCredentials = randomBoolean();
        final Authenticator.Context context;
        if (shouldExtractCredentials) {
            context = createAuthenticatorContext();
            when(oAuth2TokenAuthenticator.extractCredentials(context)).thenReturn(mock(BearerToken.class));
        } else {
            context = createAuthenticatorContext(mock(BearerToken.class));
            doCallRealMethod().when(serviceAccountAuthenticator).authenticate(eq(context), anyActionListener());
        }
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult<Authentication>> listener = (ActionListener<
                AuthenticationResult<Authentication>>) invocationOnMock.getArguments()[1];
            listener.onResponse(AuthenticationResult.success(authentication));
            return null;
        }).when(oAuth2TokenAuthenticator).authenticate(eq(context), any());
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticate(context, future);
        assertThat(future.actionGet(), is(authentication));
        if (shouldExtractCredentials) {
            verify(serviceAccountAuthenticator).extractCredentials(eq(context));
            verify(serviceAccountAuthenticator, never()).authenticate(any(), any());
            verify(oAuth2TokenAuthenticator).extractCredentials(eq(context));
        } else {
            verify(serviceAccountAuthenticator, never()).extractCredentials(any());
            verify(serviceAccountAuthenticator).authenticate(eq(context), any());
            verify(oAuth2TokenAuthenticator, never()).extractCredentials(any());
        }
        verify(oAuth2TokenAuthenticator).authenticate(eq(context), any());
        verifyNoMoreInteractions(apiKeyAuthenticator);
        verifyNoMoreInteractions(realmsAuthenticator);
        verify(authenticationContextSerializer).writeToContext(eq(authentication), any());
        verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(authentication), any());
    }

    public void testAuthenticateWithApiKey() throws IOException {
        final boolean shouldExtractCredentials = randomBoolean();
        final Authenticator.Context context;
        final SecureString apiKeySecret = new SecureString(randomAlphaOfLength(22).toCharArray());
        if (shouldExtractCredentials) {
            context = createAuthenticatorContext();
            when(apiKeyAuthenticator.extractCredentials(context)).thenReturn(
                new ApiKeyCredentials(randomAlphaOfLength(20), apiKeySecret, ApiKey.Type.REST)
            );
        } else {
            context = createAuthenticatorContext(
                new ApiKeyCredentials(randomAlphaOfLength(20), apiKeySecret, randomFrom(ApiKey.Type.values()))
            );
            doCallRealMethod().when(serviceAccountAuthenticator).authenticate(eq(context), anyActionListener());
            doCallRealMethod().when(oAuth2TokenAuthenticator).authenticate(eq(context), anyActionListener());
        }
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult<Authentication>> listener = (ActionListener<
                AuthenticationResult<Authentication>>) invocationOnMock.getArguments()[1];
            listener.onResponse(AuthenticationResult.success(authentication));
            return null;
        }).when(apiKeyAuthenticator).authenticate(eq(context), any());
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticate(context, future);
        assertThat(future.actionGet(), is(authentication));
        if (shouldExtractCredentials) {
            verify(serviceAccountAuthenticator).extractCredentials(eq(context));
            verify(serviceAccountAuthenticator, never()).authenticate(any(), any());
            verify(oAuth2TokenAuthenticator).extractCredentials(eq(context));
            verify(oAuth2TokenAuthenticator, never()).authenticate(any(), any());
            verify(apiKeyAuthenticator).extractCredentials(eq(context));
        } else {
            verify(serviceAccountAuthenticator, never()).extractCredentials(any());
            verify(serviceAccountAuthenticator).authenticate(eq(context), any());
            verify(oAuth2TokenAuthenticator, never()).extractCredentials(any());
            verify(oAuth2TokenAuthenticator).authenticate(eq(context), any());
            verify(apiKeyAuthenticator, never()).extractCredentials(any());
        }
        verify(apiKeyAuthenticator).authenticate(eq(context), any());
        verifyNoMoreInteractions(realmsAuthenticator);
        verify(authenticationContextSerializer).writeToContext(eq(authentication), any());
        verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(authentication), any());
    }

    public void testAuthenticateWithRealms() throws IOException {
        final boolean shouldExtractCredentials = randomBoolean();
        final Authenticator.Context context;
        if (shouldExtractCredentials) {
            context = createAuthenticatorContext();
            when(realmsAuthenticator.extractCredentials(context)).thenReturn(mock(AuthenticationToken.class));
        } else {
            context = createAuthenticatorContext(mock(AuthenticationToken.class));
            doCallRealMethod().when(serviceAccountAuthenticator).authenticate(eq(context), anyActionListener());
            doCallRealMethod().when(oAuth2TokenAuthenticator).authenticate(eq(context), anyActionListener());
            doCallRealMethod().when(apiKeyAuthenticator).authenticate(eq(context), anyActionListener());
        }
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult<Authentication>> listener = (ActionListener<
                AuthenticationResult<Authentication>>) invocationOnMock.getArguments()[1];
            listener.onResponse(AuthenticationResult.success(authentication));
            return null;
        }).when(realmsAuthenticator).authenticate(eq(context), any());
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticate(context, future);
        assertThat(future.actionGet(), is(authentication));
        if (shouldExtractCredentials) {
            verify(serviceAccountAuthenticator).extractCredentials(eq(context));
            verify(serviceAccountAuthenticator, never()).authenticate(any(), any());
            verify(oAuth2TokenAuthenticator).extractCredentials(eq(context));
            verify(oAuth2TokenAuthenticator, never()).authenticate(any(), any());
            verify(apiKeyAuthenticator).extractCredentials(eq(context));
            verify(apiKeyAuthenticator, never()).authenticate(any(), any());
            verify(realmsAuthenticator).extractCredentials(eq(context));
        } else {
            verify(serviceAccountAuthenticator, never()).extractCredentials(any());
            verify(serviceAccountAuthenticator).authenticate(eq(context), any());
            verify(oAuth2TokenAuthenticator, never()).extractCredentials(any());
            verify(oAuth2TokenAuthenticator).authenticate(eq(context), any());
            verify(apiKeyAuthenticator, never()).extractCredentials(any());
            verify(apiKeyAuthenticator).authenticate(eq(context), any());
            verify(realmsAuthenticator, never()).extractCredentials(any());
        }
        verify(realmsAuthenticator).authenticate(eq(context), any());
        verify(authenticationContextSerializer).writeToContext(eq(authentication), any());
        verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(authentication), any());
    }

    public void testAuthenticateFallbackAndAnonymous() throws IOException {
        final boolean hasFallbackUser = randomBoolean();
        final Authenticator.Context context;
        if (hasFallbackUser) {
            context = createAuthenticatorContext(fallbackUser);
        } else {
            context = createAuthenticatorContext();
        }
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticate(context, future);
        final Authentication authentication = future.actionGet();
        assertThat(authentication.getEffectiveSubject().getUser(), is(hasFallbackUser ? fallbackUser : anonymousUser));
        verify(serviceAccountAuthenticator).extractCredentials(eq(context));
        verify(serviceAccountAuthenticator, never()).authenticate(any(), any());
        verify(oAuth2TokenAuthenticator).extractCredentials(eq(context));
        verify(oAuth2TokenAuthenticator, never()).authenticate(any(), any());
        verify(apiKeyAuthenticator).extractCredentials(eq(context));
        verify(apiKeyAuthenticator, never()).authenticate(any(), any());
        verify(realmsAuthenticator).extractCredentials(eq(context));
        verify(realmsAuthenticator, never()).authenticate(any(), any());
        verify(authenticationContextSerializer).writeToContext(eq(authentication), any());
        verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(authentication), any());
    }

    public void testContextWithDirectWrongTokenFailsAuthn() {
        final Authenticator.Context context = createAuthenticatorContext(mock(AuthenticationToken.class));
        doCallRealMethod().when(serviceAccountAuthenticator).authenticate(eq(context), anyActionListener());
        doCallRealMethod().when(oAuth2TokenAuthenticator).authenticate(eq(context), anyActionListener());
        doCallRealMethod().when(apiKeyAuthenticator).authenticate(eq(context), anyActionListener());
        // 1. realms do not consume the token
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult<Authentication>> listener = (ActionListener<
                AuthenticationResult<Authentication>>) invocationOnMock.getArguments()[1];
            listener.onResponse(AuthenticationResult.notHandled());
            return null;
        }).when(realmsAuthenticator).authenticate(eq(context), any());
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticate(context, future);
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("failed to authenticate"));
        // no token extraction is attempted
        verify(serviceAccountAuthenticator, never()).extractCredentials(any());
        verify(oAuth2TokenAuthenticator, never()).extractCredentials(any());
        verify(apiKeyAuthenticator, never()).extractCredentials(any());
        verify(realmsAuthenticator, never()).extractCredentials(any());
        verifyNoMoreInteractions(authenticationContextSerializer);
        verifyNoMoreInteractions(operatorPrivilegesService);
        // OR 2. realms fail the token
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult<Authentication>> listener = (ActionListener<
                AuthenticationResult<Authentication>>) invocationOnMock.getArguments()[1];
            listener.onFailure(new ElasticsearchSecurityException("token fails authn"));
            return null;
        }).when(realmsAuthenticator).authenticate(eq(context), any());
        final PlainActionFuture<Authentication> future2 = new PlainActionFuture<>();
        authenticatorChain.authenticate(context, future2);
        final ElasticsearchSecurityException e2 = expectThrows(ElasticsearchSecurityException.class, future2::actionGet);
        assertThat(e2.getMessage(), containsString("token fails authn"));
        // no token extraction is attempted
        verify(serviceAccountAuthenticator, never()).extractCredentials(any());
        verify(oAuth2TokenAuthenticator, never()).extractCredentials(any());
        verify(apiKeyAuthenticator, never()).extractCredentials(any());
        verify(realmsAuthenticator, never()).extractCredentials(any());
        verifyNoMoreInteractions(authenticationContextSerializer);
        verifyNoMoreInteractions(operatorPrivilegesService);
        // OR 3. realms do not authenticate the token
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult<Authentication>> listener = (ActionListener<
                AuthenticationResult<Authentication>>) invocationOnMock.getArguments()[1];
            if (randomBoolean()) {
                listener.onResponse(
                    AuthenticationResult.terminate("not authenticated", new ElasticsearchSecurityException("failed to authenticate"))
                );
            } else {
                listener.onResponse(AuthenticationResult.unsuccessful("not authenticated", null));
            }
            return null;
        }).when(realmsAuthenticator).authenticate(eq(context), any());
        final PlainActionFuture<Authentication> future3 = new PlainActionFuture<>();
        authenticatorChain.authenticate(context, future3);
        final ElasticsearchSecurityException e3 = expectThrows(ElasticsearchSecurityException.class, future3::actionGet);
        assertThat(e3.getMessage(), containsString("failed to authenticate"));
        // no token extraction is attempted
        verify(serviceAccountAuthenticator, never()).extractCredentials(any());
        verify(oAuth2TokenAuthenticator, never()).extractCredentials(any());
        verify(apiKeyAuthenticator, never()).extractCredentials(any());
        verify(realmsAuthenticator, never()).extractCredentials(any());
        verifyNoMoreInteractions(authenticationContextSerializer);
        verifyNoMoreInteractions(operatorPrivilegesService);
    }

    public void testUnsuccessfulOAuth2TokenOrApiKeyWillNotFallToAnonymousOrReportMissingCredentials() {
        final boolean unsuccessfulApiKey = randomBoolean();
        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        when(auditableRequest.anonymousAccessDenied()).thenReturn(new ElasticsearchSecurityException("fail"));
        final Authenticator.Context context = new Authenticator.Context(threadContext, auditableRequest, null, true, realms);
        threadContext.putHeader("Authorization", unsuccessfulApiKey ? "ApiKey key_id:key_secret" : "Bearer some_token_value");
        if (unsuccessfulApiKey) {
            when(apiKeyAuthenticator.extractCredentials(context)).thenReturn(
                new ApiKeyCredentials(randomAlphaOfLength(20), new SecureString(randomAlphaOfLength(22).toCharArray()), ApiKey.Type.REST)
            );
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<AuthenticationResult<Authentication>> listener = (ActionListener<
                    AuthenticationResult<Authentication>>) invocationOnMock.getArguments()[1];
                listener.onResponse(AuthenticationResult.unsuccessful("unsuccessful api key", null));
                return null;
            }).when(apiKeyAuthenticator).authenticate(eq(context), any());
        } else {
            when(oAuth2TokenAuthenticator.extractCredentials(context)).thenReturn(mock(BearerToken.class));
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<AuthenticationResult<Authentication>> listener = (ActionListener<
                    AuthenticationResult<Authentication>>) invocationOnMock.getArguments()[1];
                listener.onResponse(AuthenticationResult.unsuccessful("unsuccessful bearer token", null));
                return null;
            }).when(oAuth2TokenAuthenticator).authenticate(eq(context), any());
        }

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticate(context, future);
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(
            e.getMessage(),
            containsString("unable to authenticate with provided credentials and anonymous access is not allowed for this request")
        );
        assertThat(
            e.getMetadata("es.additional_unsuccessful_credentials"),
            hasItem(containsString(unsuccessfulApiKey ? "unsuccessful api key" : "unsuccessful bearer token"))
        );
    }

    public void testMaybeLookupRunAsUser() {
        final Authentication authentication = randomFrom(
            AuthenticationTestHelper.builder().realm().build(false),
            AuthenticationTestHelper.builder().realm().build(false).token(),
            AuthenticationTestHelper.builder().apiKey().build(false),
            AuthenticationTestHelper.builder().apiKey().build(false).token()
        );
        final String runAsUsername = "your-run-as-username";
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, runAsUsername);
        assertThat(authentication.getEffectiveSubject().getUser().principal(), not(equalTo(runAsUsername)));

        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        final Authenticator.Context context = new Authenticator.Context(threadContext, auditableRequest, null, true, realms);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Tuple<User, Realm>> listener = (ActionListener<Tuple<User, Realm>>) invocation.getArguments()[2];
            listener.onResponse(null);
            return null;
        }).when(realmsAuthenticator).lookupRunAsUser(any(), any(), any());
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.maybeLookupRunAsUser(context, authentication, future);
        future.actionGet();
        verify(realmsAuthenticator).lookupRunAsUser(eq(context), eq(authentication), any());
    }

    public void testRunAsIsIgnoredForUnsupportedAuthenticationTypes() throws IllegalAccessException {
        final Authentication authentication = randomFrom(
            AuthenticationTestHelper.builder().serviceAccount().build(),
            AuthenticationTestHelper.builder().anonymous(anonymousUser).build(),
            AuthenticationTestHelper.builder().anonymous(anonymousUser).build().token(),
            AuthenticationTestHelper.builder().internal().build(),
            AuthenticationTestHelper.builder().realm().build(true),
            AuthenticationTestHelper.builder().realm().build(true).token(),
            AuthenticationTestHelper.builder().apiKey().runAs().build(),
            AuthenticationTestHelper.builder().apiKey().runAs().build().token()
        );
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "you-shall-not-pass");
        assertThat(
            authentication.getEffectiveSubject().getUser().principal(),
            not(equalTo(threadContext.getHeader(AuthenticationServiceField.RUN_AS_USER_HEADER)))
        );

        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        final Authenticator.Context context = new Authenticator.Context(threadContext, auditableRequest, null, true, realms);

        doAnswer(invocation -> {
            fail("should not reach here");
            return null;
        }).when(realmsAuthenticator).lookupRunAsUser(any(), any(), any());

        final Logger logger = LogManager.getLogger(AuthenticatorChain.class);
        Loggers.setLevel(logger, Level.INFO);

        try (var mockLog = MockLog.capture(AuthenticatorChain.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "run-as",
                    AuthenticatorChain.class.getName(),
                    Level.INFO,
                    "ignore run-as header since it is currently not supported for authentication [" + authentication + "]"
                )
            );
            final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
            authenticatorChain.maybeLookupRunAsUser(context, authentication, future);
            assertThat(future.actionGet(), equalTo(authentication));
            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(logger, Level.INFO);
        }
    }

    private Authenticator.Context createAuthenticatorContext() {
        return createAuthenticatorContext(mock(AuthenticationService.AuditableRequest.class));
    }

    private Authenticator.Context createAuthenticatorContext(AuthenticationService.AuditableRequest request) {
        return new Authenticator.Context(threadContext, request, null, true, realms);
    }

    private Authenticator.Context createAuthenticatorContext(User fallbackUser) {
        return new Authenticator.Context(threadContext, mock(AuthenticationService.AuditableRequest.class), fallbackUser, true, realms);
    }

    private Authenticator.Context createAuthenticatorContext(AuthenticationToken token) {
        return new Authenticator.Context(threadContext, mock(AuthenticationService.AuditableRequest.class), realms, token);
    }
}
