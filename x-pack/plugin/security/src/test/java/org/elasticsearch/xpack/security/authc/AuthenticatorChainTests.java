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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyCredentials;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges.OperatorPrivilegesService;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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

        authenticationContextSerializer = mock(AuthenticationContextSerializer.class);
        serviceAccountAuthenticator = mock(ServiceAccountAuthenticator.class);
        oAuth2TokenAuthenticator = mock(OAuth2TokenAuthenticator.class);
        apiKeyAuthenticator = mock(ApiKeyAuthenticator.class);
        realmsAuthenticator = mock(RealmsAuthenticator.class);
        when(serviceAccountAuthenticator.canBeFollowedByNullTokenHandler()).thenReturn(true);
        when(oAuth2TokenAuthenticator.canBeFollowedByNullTokenHandler()).thenReturn(true);
        when(apiKeyAuthenticator.canBeFollowedByNullTokenHandler()).thenReturn(true);
        when(realmsAuthenticator.canBeFollowedByNullTokenHandler()).thenCallRealMethod();
        when(realms.getActiveRealms()).thenReturn(org.elasticsearch.core.List.of(mock(Realm.class)));
        when(realms.getUnlicensedRealms()).thenReturn(org.elasticsearch.core.List.of());
        final User user = new User(randomAlphaOfLength(8));
        authentication = mock(Authentication.class);
        when(authentication.getUser()).thenReturn(user);
        fallbackUser = mock(User.class);
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
        authenticatorChain.authenticateAsync(context, future);
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
        when(authenticationContextSerializer.readFromContext(any())).thenReturn(mock(Authentication.class));

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticateAsync(context, future);
        assertThat(expectThrows(ElasticsearchSecurityException.class, future::actionGet), is(e));
    }

    public void testAuthenticateWithServiceAccount() throws IOException {
        final Authenticator.Context context = createAuthenticatorContext();
        when(serviceAccountAuthenticator.extractCredentials(context)).thenReturn(mock(ServiceAccountToken.class));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Authenticator.Result> listener = (ActionListener<Authenticator.Result>) invocationOnMock.getArguments()[1];
            listener.onResponse(Authenticator.Result.success(authentication));
            return null;
        }).when(serviceAccountAuthenticator).authenticate(eq(context), any());

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticateAsync(context, future);
        assertThat(future.actionGet(), is(authentication));
        verifyNoMoreInteractions(oAuth2TokenAuthenticator);
        verifyNoMoreInteractions(apiKeyAuthenticator);
        verifyNoMoreInteractions(realmsAuthenticator);
        verify(authenticationContextSerializer).writeToContext(eq(authentication), any());
        verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(authentication), any());
    }

    public void testAuthenticateWithOAuth2Token() throws IOException {
        final Authenticator.Context context = createAuthenticatorContext();
        when(oAuth2TokenAuthenticator.extractCredentials(context)).thenReturn(mock(BearerToken.class));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Authenticator.Result> listener = (ActionListener<Authenticator.Result>) invocationOnMock.getArguments()[1];
            listener.onResponse(Authenticator.Result.success(authentication));
            return null;
        }).when(oAuth2TokenAuthenticator).authenticate(eq(context), any());

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticateAsync(context, future);
        assertThat(future.actionGet(), is(authentication));
        verify(serviceAccountAuthenticator).extractCredentials(eq(context));
        verify(serviceAccountAuthenticator, never()).authenticate(eq(context), any());
        verifyNoMoreInteractions(apiKeyAuthenticator);
        verifyNoMoreInteractions(realmsAuthenticator);
        verify(authenticationContextSerializer).writeToContext(eq(authentication), any());
        verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(authentication), any());
    }

    public void testAuthenticateWithApiKey() throws IOException {
        final Authenticator.Context context = createAuthenticatorContext();
        when(apiKeyAuthenticator.extractCredentials(context)).thenReturn(
            new ApiKeyCredentials(randomAlphaOfLength(20), new SecureString(randomAlphaOfLength(22).toCharArray()))
        );
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Authenticator.Result> listener = (ActionListener<Authenticator.Result>) invocationOnMock.getArguments()[1];
            listener.onResponse(Authenticator.Result.success(authentication));
            return null;
        }).when(apiKeyAuthenticator).authenticate(eq(context), any());

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticateAsync(context, future);
        assertThat(future.actionGet(), is(authentication));
        verify(serviceAccountAuthenticator).extractCredentials(eq(context));
        verify(serviceAccountAuthenticator, never()).authenticate(eq(context), any());
        verify(oAuth2TokenAuthenticator).extractCredentials(eq(context));
        verify(oAuth2TokenAuthenticator, never()).authenticate(eq(context), any());
        verifyNoMoreInteractions(realmsAuthenticator);
        verify(authenticationContextSerializer).writeToContext(eq(authentication), any());
        verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(authentication), any());
    }

    public void testAuthenticateWithRealms() throws IOException {
        final Authenticator.Context context = createAuthenticatorContext();
        when(realmsAuthenticator.extractCredentials(context)).thenReturn(mock(AuthenticationToken.class));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Authenticator.Result> listener = (ActionListener<Authenticator.Result>) invocationOnMock.getArguments()[1];
            listener.onResponse(Authenticator.Result.success(authentication));
            return null;
        }).when(realmsAuthenticator).authenticate(eq(context), any());

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticateAsync(context, future);
        assertThat(future.actionGet(), is(authentication));
        verify(serviceAccountAuthenticator).extractCredentials(eq(context));
        verify(serviceAccountAuthenticator, never()).authenticate(eq(context), any());
        verify(oAuth2TokenAuthenticator).extractCredentials(eq(context));
        verify(oAuth2TokenAuthenticator, never()).authenticate(eq(context), any());
        verify(apiKeyAuthenticator).extractCredentials(eq(context));
        verify(apiKeyAuthenticator, never()).authenticate(eq(context), any());
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

        authenticatorChain.authenticateAsync(context, future);
        final Authentication authentication = future.actionGet();
        assertThat(authentication.getUser(), is(hasFallbackUser ? fallbackUser : anonymousUser));
        verify(serviceAccountAuthenticator).extractCredentials(eq(context));
        verify(serviceAccountAuthenticator, never()).authenticate(eq(context), any());
        verify(oAuth2TokenAuthenticator).extractCredentials(eq(context));
        verify(oAuth2TokenAuthenticator, never()).authenticate(eq(context), any());
        verify(apiKeyAuthenticator).extractCredentials(eq(context));
        verify(apiKeyAuthenticator, never()).authenticate(eq(context), any());
        verify(realmsAuthenticator).extractCredentials(eq(context));
        verify(realmsAuthenticator, never()).authenticate(eq(context), any());
        verify(authenticationContextSerializer).writeToContext(eq(authentication), any());
        verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(authentication), any());
    }

    public void testUnsuccessfulOAuth2TokenOrApiKeyWillNotFallToAnonymousOrReportMissingCredentials() {
        final boolean unsuccessfulApiKey = randomBoolean();
        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        when(auditableRequest.anonymousAccessDenied()).thenReturn(new ElasticsearchSecurityException("fail"));
        final Authenticator.Context context = new Authenticator.Context(threadContext, auditableRequest, null, true, realms);
        threadContext.putHeader("Authorization", unsuccessfulApiKey ? "ApiKey key_id:key_secret" : "Bearer some_token_value");
        if (unsuccessfulApiKey) {
            when(apiKeyAuthenticator.extractCredentials(context)).thenReturn(
                new ApiKeyCredentials(randomAlphaOfLength(20), new SecureString(randomAlphaOfLength(22).toCharArray()))
            );
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<Authenticator.Result> listener = (ActionListener<Authenticator.Result>) invocationOnMock
                    .getArguments()[1];
                listener.onResponse(Authenticator.Result.unsuccessful("unsuccessful api key", null));
                return null;
            }).when(apiKeyAuthenticator).authenticate(eq(context), any());
        } else {
            when(oAuth2TokenAuthenticator.extractCredentials(context)).thenReturn(mock(BearerToken.class));
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<Authenticator.Result> listener = (ActionListener<Authenticator.Result>) invocationOnMock
                    .getArguments()[1];
                listener.onResponse(Authenticator.Result.unsuccessful("unsuccessful bearer token", null));
                return null;
            }).when(oAuth2TokenAuthenticator).authenticate(eq(context), any());
        }

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        authenticatorChain.authenticateAsync(context, future);
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(
            e.getMessage(),
            containsString("" + "unable to authenticate with provided credentials and anonymous access is not allowed for this request")
        );
        assertThat(
            e.getMetadata("es.additional_unsuccessful_credentials"),
            hasItem(containsString(unsuccessfulApiKey ? "unsuccessful api key" : "unsuccessful bearer token"))
        );
    }

    private Authenticator.Context createAuthenticatorContext() {
        return createAuthenticatorContext(mock(AuthenticationService.AuditableRequest.class));
    }

    private Authenticator.Context createAuthenticatorContext(AuthenticationService.AuditableRequest request) {
        return createAuthenticatorContext(request, null);
    }

    private Authenticator.Context createAuthenticatorContext(User fallbackUser) {
        return new Authenticator.Context(threadContext, mock(AuthenticationService.AuditableRequest.class), fallbackUser, true, realms);
    }

    private Authenticator.Context createAuthenticatorContext(AuthenticationService.AuditableRequest request, User fallbackUser) {
        return new Authenticator.Context(threadContext, request, fallbackUser, true, realms);
    }
}
