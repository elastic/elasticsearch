/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestMatchers;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmConfig.RealmIdentifier;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.test.SecurityMocks;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator.SECONDARY_AUTH_HEADER_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecondaryAuthenticatorTests extends ESTestCase {

    private AuthenticationService authenticationService;
    private SecondaryAuthenticator authenticator;
    private DummyUsernamePasswordRealm realm;
    private ThreadPool threadPool;
    private SecurityContext securityContext;
    private TokenService tokenService;
    private Client client;

    @Before
    public void setupMocks() throws Exception {
        threadPool = new TestThreadPool(getTestName());
        final ThreadContext threadContext = threadPool.getThreadContext();

        final Realms realms = mock(Realms.class);
        final Settings settings = Settings.builder()
            .put(buildEnvSettings(Settings.EMPTY))
            .put("xpack.security.authc.realms.dummy.test_realm.order", 1)
            .put("xpack.security.authc.token.enabled", true)
            .put("xpack.security.authc.api_key.enabled", false)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);

        realm = new DummyUsernamePasswordRealm(new RealmConfig(new RealmIdentifier("dummy", "test_realm"), settings, env, threadContext));
        when(realms.asList()).thenReturn(List.of(realm));
        when(realms.getUnlicensedRealms()).thenReturn(List.of());

        final AuditTrailService auditTrail = new AuditTrailService(Collections.emptyList(), null);
        final AuthenticationFailureHandler failureHandler = new DefaultAuthenticationFailureHandler(Map.of());
        final AnonymousUser anonymous = new AnonymousUser(settings);

        final SecurityIndexManager securityIndex = SecurityMocks.mockSecurityIndexManager(RestrictedIndicesNames.SECURITY_MAIN_ALIAS);
        final SecurityIndexManager tokensIndex = SecurityMocks.mockSecurityIndexManager(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS);

        client = Mockito.mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        final TestUtils.UpdatableLicenseState licenseState = new TestUtils.UpdatableLicenseState();
        licenseState.update(License.OperationMode.PLATINUM, true, null);

        final Clock clock = Clock.systemUTC();

        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = ClusterState.EMPTY_STATE;
        when(clusterService.state()).thenReturn(clusterState);

        securityContext = new SecurityContext(settings, threadContext);

        tokenService = new TokenService(settings, clock, client, licenseState, securityContext, securityIndex, tokensIndex, clusterService);
        final ApiKeyService apiKeyService = new ApiKeyService(settings, clock, client, licenseState,
            securityIndex, clusterService, threadPool);
        authenticationService = new AuthenticationService(settings, realms, auditTrail, failureHandler, threadPool, anonymous,
            tokenService, apiKeyService);
        authenticator = new SecondaryAuthenticator(securityContext, authenticationService);
    }

    @After
    public void cleanupMocks() throws Exception {
        threadPool.shutdownNow();
    }

    public void testAuthenticateTransportRequestIsANoOpIfHeaderIsMissing() throws Exception {
        final TransportRequest request = new AuthenticateRequest();
        final PlainActionFuture<SecondaryAuthentication> future = new PlainActionFuture<>();
        authenticator.authenticate(AuthenticateAction.NAME, request, future);

        assertThat(future.get(0, TimeUnit.MILLISECONDS), nullValue());
    }

    public void testAuthenticateRestRequestIsANoOpIfHeaderIsMissing() throws Exception {
        final RestRequest request = new FakeRestRequest();
        final PlainActionFuture<SecondaryAuthentication> future = new PlainActionFuture<>();
        authenticator.authenticateAndAttachToContext(request, future);

        assertThat(future.get(0, TimeUnit.MILLISECONDS), nullValue());
        assertThat(SecondaryAuthentication.readFromContext(securityContext), nullValue());
    }

    public void testAuthenticateTransportRequestFailsIfHeaderHasUnrecognizedCredentials() throws Exception {
        threadPool.getThreadContext().putHeader(SECONDARY_AUTH_HEADER_NAME, "Fake " + randomAlphaOfLengthBetween(5, 30));
        final TransportRequest request = new AuthenticateRequest();
        final PlainActionFuture<SecondaryAuthentication> future = new PlainActionFuture<>();
        authenticator.authenticate(AuthenticateAction.NAME, request, future);

        final ElasticsearchSecurityException ex = expectThrows(ElasticsearchSecurityException.class,
            () -> future.actionGet(0, TimeUnit.MILLISECONDS));
        assertThat(ex, TestMatchers.throwableWithMessage(Matchers.containsString("secondary user")));
        assertThat(ex.getCause(), TestMatchers.throwableWithMessage(Matchers.containsString("credentials")));
    }

    public void testAuthenticateRestRequestFailsIfHeaderHasUnrecognizedCredentials() throws Exception {
        threadPool.getThreadContext().putHeader(SECONDARY_AUTH_HEADER_NAME, "Fake " + randomAlphaOfLengthBetween(5, 30));
        final RestRequest request = new FakeRestRequest();
        final PlainActionFuture<SecondaryAuthentication> future = new PlainActionFuture<>();
        authenticator.authenticateAndAttachToContext(request, future);

        final ElasticsearchSecurityException ex = expectThrows(ElasticsearchSecurityException.class,
            () -> future.actionGet(0, TimeUnit.MILLISECONDS));
        assertThat(ex, TestMatchers.throwableWithMessage(Matchers.containsString("secondary user")));
        assertThat(ex.getCause(), TestMatchers.throwableWithMessage(Matchers.containsString("credentials")));

        assertThat(SecondaryAuthentication.readFromContext(securityContext), nullValue());
    }

    public void testAuthenticateTransportRequestSucceedsWithBasicAuthentication() throws Exception {
        assertAuthenticateWithBasicAuthentication(listener -> {
            final TransportRequest request = new AuthenticateRequest();
            authenticator.authenticate(AuthenticateAction.NAME, request, listener);
        });
    }

    public void testAuthenticateRestRequestSucceedsWithBasicAuthentication() throws Exception {
        final SecondaryAuthentication secondaryAuthentication = assertAuthenticateWithBasicAuthentication(listener -> {
            final RestRequest request = new FakeRestRequest();
            authenticator.authenticateAndAttachToContext(request, listener);
        });
        assertThat(SecondaryAuthentication.readFromContext(securityContext), equalTo(secondaryAuthentication));
    }

    private SecondaryAuthentication assertAuthenticateWithBasicAuthentication(Consumer<ActionListener<SecondaryAuthentication>> consumer)
        throws Exception {
        final String user = randomAlphaOfLengthBetween(6, 12);
        final SecureString password = new SecureString(randomAlphaOfLengthBetween(8, 24).toCharArray());
        realm.defineUser(user, password);

        threadPool.getThreadContext().putHeader(SECONDARY_AUTH_HEADER_NAME, "Basic " +
            Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)));

        final PlainActionFuture<SecondaryAuthentication> future = new PlainActionFuture<>();
        final AtomicReference<ThreadContext.StoredContext> listenerContext = new AtomicReference<>();
        consumer.accept(ActionListener.wrap(
            result -> {
                listenerContext.set(securityContext.getThreadContext().newStoredContext(false));
                future.onResponse(result);
            },
            e -> future.onFailure(e)
        ));

        final SecondaryAuthentication secondaryAuthentication = future.get(0, TimeUnit.MILLISECONDS);
        assertThat(secondaryAuthentication, Matchers.notNullValue());
        assertThat(secondaryAuthentication.getAuthentication(), Matchers.notNullValue());
        assertThat(secondaryAuthentication.getAuthentication().getUser().principal(), equalTo(user));
        assertThat(secondaryAuthentication.getAuthentication().getAuthenticatedBy().getName(), equalTo(realm.name()));

        listenerContext.get().restore();
        return secondaryAuthentication;
    }

    public void testAuthenticateTransportRequestFailsWithIncorrectPassword() throws Exception {
        assertAuthenticateWithIncorrectPassword(listener -> {
            final TransportRequest request = new AuthenticateRequest();
            authenticator.authenticate(AuthenticateAction.NAME, request, listener);
        });
    }

    public void testAuthenticateRestRequestFailsWithIncorrectPassword() throws Exception {
        assertAuthenticateWithIncorrectPassword(listener -> {
            final RestRequest request = new FakeRestRequest();
            authenticator.authenticateAndAttachToContext(request, listener);
        });
        assertThat(SecondaryAuthentication.readFromContext(securityContext), nullValue());
    }

    private void assertAuthenticateWithIncorrectPassword(Consumer<ActionListener<SecondaryAuthentication>> consumer) {
        final String user = randomAlphaOfLengthBetween(6, 12);
        final SecureString password = new SecureString(randomAlphaOfLengthBetween(8, 24).toCharArray());
        realm.defineUser(user, password);

        threadPool.getThreadContext().putHeader(SECONDARY_AUTH_HEADER_NAME, "Basic " +
            Base64.getEncoder().encodeToString((user + ":NOT-" + password).getBytes(StandardCharsets.UTF_8)));

        final PlainActionFuture<SecondaryAuthentication> future = new PlainActionFuture<>();
        final AtomicReference<ThreadContext.StoredContext> listenerContext = new AtomicReference<>();
        consumer.accept(ActionListener.wrap(
            future::onResponse,
            e -> {
                listenerContext.set(securityContext.getThreadContext().newStoredContext(false));
                future.onFailure(e);
            }
        ));

        final ElasticsearchSecurityException ex = expectThrows(ElasticsearchSecurityException.class,
            () -> future.actionGet(0, TimeUnit.MILLISECONDS));

        assertThat(ex, TestMatchers.throwableWithMessage(Matchers.containsString("secondary user")));
        assertThat(ex.getCause(), TestMatchers.throwableWithMessage(Matchers.containsString(user)));

        listenerContext.get().restore();
    }

    public void testAuthenticateUsingBearerToken() throws Exception {
        final User user = new User(randomAlphaOfLengthBetween(6, 12));
        Authentication auth = new Authentication(user,
            new RealmRef(randomAlphaOfLengthBetween(4, 8), randomAlphaOfLengthBetween(3, 6), randomAlphaOfLengthBetween(8, 12)),
            null);

        final AtomicReference<String> tokenDocId = new AtomicReference<>();
        final AtomicReference<BytesReference> tokenSource = new AtomicReference<>();
        SecurityMocks.mockIndexRequest(client, RestrictedIndicesNames.SECURITY_TOKENS_ALIAS, request -> {
            tokenDocId.set(request.id());
            tokenSource.set(request.source());
        });

        final PlainActionFuture<Tuple<String, String>> tokenFuture = new PlainActionFuture<>();
        tokenService.createOAuth2Tokens(auth, auth, Map.of(), false, tokenFuture);
        final String token = tokenFuture.actionGet().v1();

        threadPool.getThreadContext().putHeader(SECONDARY_AUTH_HEADER_NAME, "Bearer " + token);

        SecurityMocks.mockGetRequest(client, RestrictedIndicesNames.SECURITY_TOKENS_ALIAS, tokenDocId.get(), tokenSource.get());

        final TransportRequest request = new AuthenticateRequest();
        final PlainActionFuture<SecondaryAuthentication> future = new PlainActionFuture<>();
        authenticator.authenticate(AuthenticateAction.NAME, request, future);

        final SecondaryAuthentication secondaryAuthentication = future.actionGet(0, TimeUnit.MILLISECONDS);
        assertThat(secondaryAuthentication, Matchers.notNullValue());
        assertThat(secondaryAuthentication.getAuthentication(), Matchers.notNullValue());
        assertThat(secondaryAuthentication.getAuthentication().getUser(), equalTo(user));
        assertThat(secondaryAuthentication.getAuthentication().getAuthenticationType(), equalTo(AuthenticationType.TOKEN));
    }

}
