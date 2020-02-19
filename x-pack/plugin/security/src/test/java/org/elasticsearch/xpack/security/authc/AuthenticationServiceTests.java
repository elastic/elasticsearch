/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.Realm.Factory;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.EmptyAuthorizationInfo;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.AuthenticationService.Authenticator;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;
import static org.elasticsearch.xpack.security.authc.TokenServiceTests.mockGetTokenFromId;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;


/**
 * Unit tests for the {@link AuthenticationService}
 */
public class AuthenticationServiceTests extends ESTestCase {

    private static final String SECOND_REALM_NAME = "second_realm";
    private static final String SECOND_REALM_TYPE = "second";
    private static final String FIRST_REALM_NAME = "file_realm";
    private static final String FIRST_REALM_TYPE = "file";
    private AuthenticationService service;
    private TransportMessage message;
    private RestRequest restRequest;
    private Realms realms;
    private Realm firstRealm;
    private Realm secondRealm;
    private AuditTrailService auditTrail;
    private AuthenticationToken token;
    private ThreadPool threadPool;
    private ThreadContext threadContext;
    private TokenService tokenService;
    private ApiKeyService apiKeyService;
    private SecurityIndexManager securityIndex;
    private Client client;
    private InetSocketAddress remoteAddress;

    private String concreteSecurityIndexName;

    @Before
    @SuppressForbidden(reason = "Allow accessing localhost")
    public void init() throws Exception {
        concreteSecurityIndexName = randomFrom(
            RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_6, RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7);

        token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        message = new InternalMessage();
        remoteAddress = new InetSocketAddress(InetAddress.getLocalHost(), 100);
        message.remoteAddress(new TransportAddress(remoteAddress));
        restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withRemoteAddress(remoteAddress).build();
        threadContext = new ThreadContext(Settings.EMPTY);

        firstRealm = mock(Realm.class);
        when(firstRealm.type()).thenReturn(FIRST_REALM_TYPE);
        when(firstRealm.name()).thenReturn(FIRST_REALM_NAME);
        secondRealm = mock(Realm.class);
        when(secondRealm.type()).thenReturn(SECOND_REALM_TYPE);
        when(secondRealm.name()).thenReturn(SECOND_REALM_NAME);
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("node.name", "authc_test")
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true)
            .build();
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.allowedRealmType()).thenReturn(XPackLicenseState.AllowedRealmType.ALL);
        when(licenseState.isAuthAllowed()).thenReturn(true);
        when(licenseState.isApiKeyServiceAllowed()).thenReturn(true);
        when(licenseState.isTokenServiceAllowed()).thenReturn(true);
        ReservedRealm reservedRealm = mock(ReservedRealm.class);
        when(reservedRealm.type()).thenReturn("reserved");
        when(reservedRealm.name()).thenReturn("reserved_realm");
        realms = spy(new TestRealms(Settings.EMPTY, TestEnvironment.newEnvironment(settings), Collections.<String, Realm.Factory>emptyMap(),
                licenseState, threadContext, reservedRealm, Arrays.asList(firstRealm, secondRealm),
                Collections.singletonList(firstRealm)));

        auditTrail = mock(AuditTrailService.class);
        client = mock(Client.class);
        threadPool = new ThreadPool(settings,
                new FixedExecutorBuilder(settings, TokenService.THREAD_POOL_NAME, 1, 1000, "xpack.security.authc.token.thread_pool"));
        threadContext = threadPool.getThreadContext();
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        when(client.prepareIndex(any(String.class)))
            .thenReturn(new IndexRequestBuilder(client, IndexAction.INSTANCE));
        when(client.prepareUpdate(any(String.class), any(String.class)))
            .thenReturn(new UpdateRequestBuilder(client, UpdateAction.INSTANCE));
        doAnswer(invocationOnMock -> {
            ActionListener<IndexResponse> responseActionListener = (ActionListener<IndexResponse>) invocationOnMock.getArguments()[2];
            responseActionListener.onResponse(new IndexResponse(new ShardId(".security", UUIDs.randomBase64UUID(), randomInt()),
                    randomAlphaOfLength(4), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), true));
            return null;
        }).when(client).execute(eq(IndexAction.INSTANCE), any(IndexRequest.class), any(ActionListener.class));
        doAnswer(invocationOnMock -> {
            GetRequestBuilder builder = new GetRequestBuilder(client, GetAction.INSTANCE);
            builder.setIndex((String) invocationOnMock.getArguments()[0])
                    .setId((String) invocationOnMock.getArguments()[1]);
            return builder;
        }).when(client).prepareGet(anyString(), anyString());
        securityIndex = mock(SecurityIndexManager.class);
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(any(Consumer.class), any(Runnable.class));
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(securityIndex).checkIndexVersionThenExecute(any(Consumer.class), any(Runnable.class));
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        final SecurityContext securityContext = new SecurityContext(settings, threadContext);
        apiKeyService = new ApiKeyService(settings, Clock.systemUTC(), client, licenseState, securityIndex, clusterService, threadPool);
        tokenService = new TokenService(settings, Clock.systemUTC(), client, licenseState, securityContext, securityIndex, securityIndex,
            clusterService);
        service = new AuthenticationService(settings, realms, auditTrail, new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool, new AnonymousUser(settings), tokenService, apiKeyService);
    }

    @After
    public void shutdownThreadpool() throws InterruptedException {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }

    public void testTokenFirstMissingSecondFound() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(token);

        PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        Authenticator authenticator = service.createAuthenticator("_action", message, null, future);
        authenticator.extractToken((result) -> {
            assertThat(result, notNullValue());
            assertThat(result, is(token));
            verifyZeroInteractions(auditTrail);
        });
    }

    public void testTokenMissing() throws Exception {
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        Authenticator authenticator = service.createAuthenticator("_action", message, null, future);
        authenticator.extractToken((token) -> {
            assertThat(token, nullValue());
            authenticator.handleNullToken();
        });

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> future.actionGet());
        assertThat(e.getMessage(), containsString("missing authentication credentials"));
        verify(auditTrail).anonymousAccessDenied(reqId, "_action", message);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthenticateBothSupportSecondSucceeds() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, null);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);
        if (randomBoolean()) {
            when(firstRealm.token(threadContext)).thenReturn(token);
        } else {
            when(secondRealm.token(threadContext)).thenReturn(token);
        }
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);

        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate("_action", message, (User)null, ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getLookedUpBy(), is(nullValue()));
            assertThat(result.getAuthenticatedBy(), is(notNullValue())); // TODO implement equals
            assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
        }, this::logAndFail));
        assertTrue(completed.get());
        verify(auditTrail).authenticationFailed(reqId, firstRealm.name(), token, "_action", message);
        verify(realms).asList();
        verifyNoMoreInteractions(realms);
    }

    public void testAuthenticateSmartRealmOrdering() {
        User user = new User("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, null);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);
        when(secondRealm.token(threadContext)).thenReturn(token);
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);

        // Authenticate against the normal chain. 1st Realm will be checked (and not pass) then 2nd realm will successfully authc
        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate("_action", message, (User)null, ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getLookedUpBy(), is(nullValue()));
            assertThat(result.getAuthenticatedBy(), is(notNullValue())); // TODO implement equals
            assertThat(result.getAuthenticatedBy().getName(), is(SECOND_REALM_NAME));
            assertThat(result.getAuthenticatedBy().getType(), is(SECOND_REALM_TYPE));
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
        }, this::logAndFail));
        assertTrue(completed.get());

        completed.set(false);
        // Authenticate against the smart chain.
        // "SecondRealm" will be at the top of the list and will successfully authc.
        // "FirstRealm" will not be used
        service.authenticate("_action", message, (User)null, ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getLookedUpBy(), is(nullValue()));
            assertThat(result.getAuthenticatedBy(), is(notNullValue())); // TODO implement equals
            assertThat(result.getAuthenticatedBy().getName(), is(SECOND_REALM_NAME));
            assertThat(result.getAuthenticatedBy().getType(), is(SECOND_REALM_TYPE));
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
        }, this::logAndFail));

        verify(auditTrail).authenticationFailed(reqId, firstRealm.name(), token, "_action", message);
        verify(auditTrail, times(2)).authenticationSuccess(reqId, secondRealm.name(), user, "_action", message);
        verify(firstRealm, times(2)).name(); // used above one time
        verify(secondRealm, times(3)).name(); // used above one time
        verify(secondRealm, times(2)).type(); // used to create realm ref
        verify(firstRealm, times(2)).token(threadContext);
        verify(secondRealm, times(2)).token(threadContext);
        verify(firstRealm).supports(token);
        verify(secondRealm, times(2)).supports(token);
        verify(firstRealm).authenticate(eq(token), any(ActionListener.class));
        verify(secondRealm, times(2)).authenticate(eq(token), any(ActionListener.class));
        verifyNoMoreInteractions(auditTrail, firstRealm, secondRealm);

        // Now assume some change in the backend system so that 2nd realm no longer has the user, but the 1st realm does.
        mockAuthenticate(secondRealm, token, null);
        mockAuthenticate(firstRealm, token, user);

        completed.set(false);
        // This will authenticate against the smart chain.
        // "SecondRealm" will be at the top of the list but will no longer authenticate the user.
        // Then "FirstRealm" will be checked.
        service.authenticate("_action", message, (User)null, ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getLookedUpBy(), is(nullValue()));
            assertThat(result.getAuthenticatedBy(), is(notNullValue()));
            assertThat(result.getAuthenticatedBy().getName(), is(FIRST_REALM_NAME));
            assertThat(result.getAuthenticatedBy().getType(), is(FIRST_REALM_TYPE));
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
        }, this::logAndFail));

        verify(auditTrail, times(1)).authenticationFailed(reqId, SECOND_REALM_NAME, token, "_action", message);
        verify(auditTrail, times(1)).authenticationSuccess(reqId, FIRST_REALM_NAME, user, "_action", message);
        verify(secondRealm, times(3)).authenticate(eq(token), any(ActionListener.class)); // 2 from above + 1 more
        verify(firstRealm, times(2)).authenticate(eq(token), any(ActionListener.class)); // 1 from above + 1 more
    }

    public void testCacheClearOnSecurityIndexChange() {
        long expectedInvalidation = 0L;
        assertEquals(expectedInvalidation, service.getNumInvalidation());

        // existing to no longer present
        SecurityIndexManager.State previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        SecurityIndexManager.State currentState = dummyState(null);
        service.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(++expectedInvalidation, service.getNumInvalidation());

        // doesn't exist to exists
        previousState = dummyState(null);
        currentState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        service.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(++expectedInvalidation, service.getNumInvalidation());

        // green or yellow to red
        previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentState = dummyState(ClusterHealthStatus.RED);
        service.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(expectedInvalidation, service.getNumInvalidation());

        // red to non red
        previousState = dummyState(ClusterHealthStatus.RED);
        currentState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        service.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(++expectedInvalidation, service.getNumInvalidation());

        // green to yellow or yellow to green
        previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentState = dummyState(previousState.indexHealth == ClusterHealthStatus.GREEN ?
            ClusterHealthStatus.YELLOW : ClusterHealthStatus.GREEN);
        service.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(expectedInvalidation, service.getNumInvalidation());
    }

    public void testAuthenticateSmartRealmOrderingDisabled() {
        final Settings settings = Settings.builder()
            .put(AuthenticationService.SUCCESS_AUTH_CACHE_ENABLED.getKey(), false)
            .build();
        service = new AuthenticationService(settings, realms, auditTrail,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool, new AnonymousUser(Settings.EMPTY),
            tokenService, apiKeyService);
        User user = new User("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, null);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);
        when(secondRealm.token(threadContext)).thenReturn(token);
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);

        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate("_action", message, (User)null, ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getLookedUpBy(), is(nullValue()));
            assertThat(result.getAuthenticatedBy(), is(notNullValue())); // TODO implement equals
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
        }, this::logAndFail));
        assertTrue(completed.get());

        completed.set(false);
        service.authenticate("_action", message, (User)null, ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getLookedUpBy(), is(nullValue()));
            assertThat(result.getAuthenticatedBy(), is(notNullValue())); // TODO implement equals
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
        }, this::logAndFail));
        verify(auditTrail, times(2)).authenticationFailed(reqId, firstRealm.name(), token, "_action", message);
        verify(auditTrail, times(2)).authenticationSuccess(reqId, secondRealm.name(), user, "_action", message);
        verify(firstRealm, times(3)).name(); // used above one time
        verify(secondRealm, times(3)).name(); // used above one time
        verify(secondRealm, times(2)).type(); // used to create realm ref
        verify(firstRealm, times(2)).token(threadContext);
        verify(secondRealm, times(2)).token(threadContext);
        verify(firstRealm, times(2)).supports(token);
        verify(secondRealm, times(2)).supports(token);
        verify(firstRealm, times(2)).authenticate(eq(token), any(ActionListener.class));
        verify(secondRealm, times(2)).authenticate(eq(token), any(ActionListener.class));
        verifyNoMoreInteractions(auditTrail, firstRealm, secondRealm);
    }

    public void testAuthenticateFirstNotSupportingSecondSucceeds() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(false);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);
        when(secondRealm.token(threadContext)).thenReturn(token);
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);

        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate("_action", message, (User)null, ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
        }, this::logAndFail));
        verify(auditTrail).authenticationSuccess(reqId, secondRealm.name(), user, "_action", message);
        verifyNoMoreInteractions(auditTrail);
        verify(firstRealm, never()).authenticate(eq(token), any(ActionListener.class));
        assertTrue(completed.get());
    }

    public void testAuthenticateCached() throws Exception {
        final Authentication authentication = new Authentication(new User("_username", "r1"), new RealmRef("test", "cached", "foo"), null);
        authentication.writeToContext(threadContext);

        Authentication result = authenticateBlocking("_action", message, null);

        assertThat(result, notNullValue());
        assertThat(result, is(authentication));
        assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
        verifyZeroInteractions(auditTrail);
        verifyZeroInteractions(firstRealm);
        verifyZeroInteractions(secondRealm);
    }

    public void testAuthenticateNonExistentRestRequestUserThrowsAuthenticationException() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(new UsernamePasswordToken("idonotexist",
                new SecureString("passwd".toCharArray())));
        try {
            authenticateBlocking(restRequest);
            fail("Authentication was successful but should not");
        } catch (ElasticsearchSecurityException e) {
            assertAuthenticationException(e, containsString("unable to authenticate user [idonotexist] for REST request [/]"));
        }
    }

    public void testTokenRestMissing() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);

        Authenticator authenticator = service.createAuthenticator(restRequest, mock(ActionListener.class));
        authenticator.extractToken((token) -> {
            assertThat(token, nullValue());
        });
    }

    public void authenticationInContextAndHeader() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        Authentication result = authenticateBlocking("_action", message, null);

        assertThat(result, notNullValue());
        assertThat(result.getUser(), is(user));
        assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));

        String userStr = threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY);
        assertThat(userStr, notNullValue());
        assertThat(userStr, equalTo("_signed_auth"));

        Authentication ctxAuth = threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY);
        assertThat(ctxAuth, is(result));
    }

    public void testAuthenticateTransportAnonymous() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        try {
            authenticateBlocking("_action", message, null);
            fail("expected an authentication exception when trying to authenticate an anonymous message");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthenticationException(e);
        }
        verify(auditTrail).anonymousAccessDenied(reqId, "_action", message);
    }

    public void testAuthenticateRestAnonymous()  throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);
        try {
            authenticateBlocking(restRequest);
            fail("expected an authentication exception when trying to authenticate an anonymous message");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthenticationException(e);
        }
        String reqId = expectAuditRequestId();
        verify(auditTrail).anonymousAccessDenied(reqId, restRequest);
    }

    public void testAuthenticateTransportFallback() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);
        User user1 = new User("username", "r1", "r2");

        Authentication result = authenticateBlocking("_action", message, user1);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(user1));
        assertThat(result.getAuthenticationType(), is(AuthenticationType.INTERNAL));
        assertThreadContextContainsAuthentication(result);
    }

    public void testAuthenticateTransportDisabledUser() throws Exception {
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        User user = new User("username", new String[] { "r1", "r2" }, null, null, Map.of(), false);
        User fallback = randomBoolean() ? SystemUser.INSTANCE : null;
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", message, fallback));
        verify(auditTrail).authenticationFailed(reqId, token, "_action", message);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
    }

    public void testAuthenticateRestDisabledUser() throws Exception {
        User user = new User("username", new String[] { "r1", "r2" }, null, null, Map.of(), false);
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking(restRequest));
        String reqId = expectAuditRequestId();
        verify(auditTrail).authenticationFailed(reqId, token, restRequest);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
    }

    public void testAuthenticateTransportSuccess() throws Exception {
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        User user = new User("username", "r1", "r2");
        User fallback = randomBoolean() ? SystemUser.INSTANCE : null;
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate("_action", message, fallback, ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            assertThat(result.getUser(), sameInstance(user));
            assertThreadContextContainsAuthentication(result);
            assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
            setCompletedToTrue(completed);
        }, this::logAndFail));

        verify(auditTrail).authenticationSuccess(reqId, firstRealm.name(), user, "_action", message);
        verifyNoMoreInteractions(auditTrail);
        assertTrue(completed.get());
    }

    public void testAuthenticateRestSuccess() throws Exception {
        User user1 = new User("username", "r1", "r2");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user1);
        // this call does not actually go async
        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate(restRequest, ActionListener.wrap(authentication -> {
            assertThat(authentication, notNullValue());
            assertThat(authentication.getUser(), sameInstance(user1));
            assertThat(authentication.getAuthenticationType(), is(AuthenticationType.REALM));
            assertThreadContextContainsAuthentication(authentication);
            setCompletedToTrue(completed);
        }, this::logAndFail));
        String reqId = expectAuditRequestId();
        verify(auditTrail).authenticationSuccess(reqId, firstRealm.name(), user1, restRequest);
        verifyNoMoreInteractions(auditTrail);
        assertTrue(completed.get());
    }

    public void testAuthenticateTransportContextAndHeader() throws Exception {
        User user1 = new User("username", "r1", "r2");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user1);
        final AtomicBoolean completed = new AtomicBoolean(false);
        final SetOnce<Authentication> authRef = new SetOnce<>();
        final SetOnce<String> authHeaderRef = new SetOnce<>();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            service.authenticate("_action", message, SystemUser.INSTANCE, ActionListener.wrap(authentication -> {
                assertThat(authentication, notNullValue());
                assertThat(authentication.getUser(), sameInstance(user1));
                assertThat(authentication.getAuthenticationType(), is(AuthenticationType.REALM));
                assertThreadContextContainsAuthentication(authentication);
                authRef.set(authentication);
                authHeaderRef.set(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY));
                setCompletedToTrue(completed);
            }, this::logAndFail));
        }
        assertTrue(completed.compareAndSet(true, false));
        reset(firstRealm);

        // checking authentication from the context
        InternalMessage message1 = new InternalMessage();
        ThreadPool threadPool1 = new TestThreadPool("testAutheticateTransportContextAndHeader1");
        try {
            ThreadContext threadContext1 = threadPool1.getThreadContext();
            service = new AuthenticationService(Settings.EMPTY, realms, auditTrail,
                new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool1, new AnonymousUser(Settings.EMPTY),
                tokenService, apiKeyService);

            threadContext1.putTransient(AuthenticationField.AUTHENTICATION_KEY, authRef.get());
            threadContext1.putHeader(AuthenticationField.AUTHENTICATION_KEY, authHeaderRef.get());
            service.authenticate("_action", message1, SystemUser.INSTANCE, ActionListener.wrap(ctxAuth -> {
                assertThat(ctxAuth, sameInstance(authRef.get()));
                assertThat(threadContext1.getHeader(AuthenticationField.AUTHENTICATION_KEY), sameInstance(authHeaderRef.get()));
                setCompletedToTrue(completed);
            }, this::logAndFail));
            assertTrue(completed.compareAndSet(true, false));
            verifyZeroInteractions(firstRealm);
            reset(firstRealm);
        } finally {
            terminate(threadPool1);
        }

        // checking authentication from the user header
        ThreadPool threadPool2 = new TestThreadPool("testAutheticateTransportContextAndHeader2");
        try {
            ThreadContext threadContext2 = threadPool2.getThreadContext();
            final String header;
            try (ThreadContext.StoredContext ignore = threadContext2.stashContext()) {
                service = new AuthenticationService(Settings.EMPTY, realms, auditTrail,
                    new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool2, new AnonymousUser(Settings.EMPTY),
                    tokenService, apiKeyService);
                threadContext2.putHeader(AuthenticationField.AUTHENTICATION_KEY, authHeaderRef.get());

                BytesStreamOutput output = new BytesStreamOutput();
                threadContext2.writeTo(output);
                StreamInput input = output.bytes().streamInput();
                threadContext2 = new ThreadContext(Settings.EMPTY);
                threadContext2.readHeaders(input);
                header = threadContext2.getHeader(AuthenticationField.AUTHENTICATION_KEY);
            }

            threadPool2.getThreadContext().putHeader(AuthenticationField.AUTHENTICATION_KEY, header);
            service = new AuthenticationService(Settings.EMPTY, realms, auditTrail,
                new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool2, new AnonymousUser(Settings.EMPTY),
                tokenService, apiKeyService);
            service.authenticate("_action", new InternalMessage(), SystemUser.INSTANCE, ActionListener.wrap(result -> {
                assertThat(result, notNullValue());
                assertThat(result.getUser(), equalTo(user1));
                assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
                setCompletedToTrue(completed);
            }, this::logAndFail));
            assertTrue(completed.get());
            verifyZeroInteractions(firstRealm);
        } finally {
            terminate(threadPool2);
        }
    }

    public void testAuthenticateTamperedUser() throws Exception {
        InternalMessage message = new InternalMessage();
        threadContext.putHeader(AuthenticationField.AUTHENTICATION_KEY, "_signed_auth");
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);

        try {
            authenticateBlocking("_action", message, randomBoolean() ? SystemUser.INSTANCE : null);
        } catch (Exception e) {
            //expected
            verify(auditTrail).tamperedRequest(reqId, "_action", message);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testWrongTokenDoesNotFallbackToAnonymous() {
        String username = randomBoolean() ? AnonymousUser.DEFAULT_ANONYMOUS_USERNAME : "user1";
        Settings.Builder builder = Settings.builder()
            .putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3");
        if (username.equals(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME) == false) {
            builder.put(AnonymousUser.USERNAME_SETTING.getKey(), username);
        }
        Settings anonymousEnabledSettings = builder.build();
        final AnonymousUser anonymousUser = new AnonymousUser(anonymousEnabledSettings);
        service = new AuthenticationService(anonymousEnabledSettings, realms, auditTrail,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool, anonymousUser, tokenService, apiKeyService);

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
            threadContext.putHeader("Authorization", "Bearer thisisaninvalidtoken");
            ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", message, null));
            verify(auditTrail).anonymousAccessDenied(reqId, "_action", message);
            verifyNoMoreInteractions(auditTrail);
            assertAuthenticationException(e);
        }
    }

    public void testWrongApiKeyDoesNotFallbackToAnonymous() {
        String username = randomBoolean() ? AnonymousUser.DEFAULT_ANONYMOUS_USERNAME : "user1";
        Settings.Builder builder = Settings.builder()
            .putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3");
        if (username.equals(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME) == false) {
            builder.put(AnonymousUser.USERNAME_SETTING.getKey(), username);
        }
        Settings anonymousEnabledSettings = builder.build();
        final AnonymousUser anonymousUser = new AnonymousUser(anonymousEnabledSettings);
        service = new AuthenticationService(anonymousEnabledSettings, realms, auditTrail,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool, anonymousUser, tokenService, apiKeyService);
        doAnswer(invocationOnMock -> {
            final GetRequest request = (GetRequest) invocationOnMock.getArguments()[0];
            final ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(new GetResponse(new GetResult(request.index(), request.id(),
                SequenceNumbers.UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, -1L, false, null,
                Collections.emptyMap(), Collections.emptyMap())));
            return Void.TYPE;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
            threadContext.putHeader("Authorization", "ApiKey dGhpc2lzYW5pbnZhbGlkaWQ6dGhpc2lzYW5pbnZhbGlkc2VjcmV0");
            ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", message, null));
            verify(auditTrail).anonymousAccessDenied(reqId, "_action", message);
            verifyNoMoreInteractions(auditTrail);
            assertAuthenticationException(e);
        }
    }

    public void testAnonymousUserRest() throws Exception {
        String username = randomBoolean() ? AnonymousUser.DEFAULT_ANONYMOUS_USERNAME : "user1";
        Settings.Builder builder = Settings.builder()
                .putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3");
        if (username.equals(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME) == false) {
            builder.put(AnonymousUser.USERNAME_SETTING.getKey(), username);
        }
        Settings settings = builder.build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(settings, realms, auditTrail, new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool, anonymousUser, tokenService, apiKeyService);
        RestRequest request = new FakeRestRequest();

        Authentication result = authenticateBlocking(request);

        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance((Object) anonymousUser));
        assertThat(result.getAuthenticationType(), is(AuthenticationType.ANONYMOUS));
        assertThreadContextContainsAuthentication(result);
        String reqId = expectAuditRequestId();
        verify(auditTrail).authenticationSuccess(reqId, "__anonymous", new AnonymousUser(settings), request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAnonymousUserTransportNoDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(settings, realms, auditTrail, new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool, anonymousUser, tokenService, apiKeyService);
        InternalMessage message = new InternalMessage();

        Authentication result = authenticateBlocking("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(anonymousUser));
        assertThat(result.getAuthenticationType(), is(AuthenticationType.ANONYMOUS));
        assertThreadContextContainsAuthentication(result);
    }

    public void testAnonymousUserTransportWithDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(settings, realms, auditTrail, new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool, anonymousUser, tokenService, apiKeyService);

        InternalMessage message = new InternalMessage();

        Authentication result = authenticateBlocking("_action", message, SystemUser.INSTANCE);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(SystemUser.INSTANCE));
        assertThat(result.getAuthenticationType(), is(AuthenticationType.INTERNAL));
        assertThreadContextContainsAuthentication(result);
    }

    public void testRealmTokenThrowingException() throws Exception {
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        when(firstRealm.token(threadContext)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            authenticateBlocking("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            verify(auditTrail).authenticationFailed(reqId, "_action", message);
        }
    }

    public void testRealmTokenThrowingExceptionRest() throws Exception {
        when(firstRealm.token(threadContext)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            authenticateBlocking(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            String reqId = expectAuditRequestId();
            verify(auditTrail).authenticationFailed(reqId, restRequest);
        }
    }

    public void testRealmSupportsMethodThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenThrow(authenticationError("realm doesn't like supports"));
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        try {
            authenticateBlocking("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like supports"));
            verify(auditTrail).authenticationFailed(reqId, token, "_action", message);
        }
    }

    public void testRealmSupportsMethodThrowingExceptionRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenThrow(authenticationError("realm doesn't like supports"));
        try {
            authenticateBlocking(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like supports"));
            String reqId = expectAuditRequestId();
            verify(auditTrail).authenticationFailed(reqId, token, restRequest);
        }
    }

    public void testRealmAuthenticateTerminatingAuthenticationProcess() throws Exception {
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        final AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        final boolean terminateWithNoException = rarely();
        final boolean throwElasticsearchSecurityException = (terminateWithNoException == false) && randomBoolean();
        final boolean withAuthenticateHeader = throwElasticsearchSecurityException && randomBoolean();
        Exception throwE = new Exception("general authentication error");
        final String basicScheme = "Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\"";
        String selectedScheme = randomFrom(basicScheme, "Negotiate IOJoj");
        if (throwElasticsearchSecurityException) {
            throwE = new ElasticsearchSecurityException("authentication error", RestStatus.UNAUTHORIZED);
            if (withAuthenticateHeader) {
                ((ElasticsearchSecurityException) throwE).addHeader("WWW-Authenticate", selectedScheme);
            }
        }
        mockAuthenticate(secondRealm, token, (terminateWithNoException) ? null : throwE, true);

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", message, null));
        if (terminateWithNoException) {
            assertThat(e.getMessage(), is("terminate authc process"));
            assertThat(e.getHeader("WWW-Authenticate"), contains(basicScheme));
        } else {
            if (throwElasticsearchSecurityException) {
                assertThat(e.getMessage(), is("authentication error"));
                if (withAuthenticateHeader) {
                    assertThat(e.getHeader("WWW-Authenticate"), contains(selectedScheme));
                } else {
                    assertThat(e.getHeader("WWW-Authenticate"), contains(basicScheme));
                }
            } else {
                assertThat(e.getMessage(), is("error attempting to authenticate request"));
                assertThat(e.getHeader("WWW-Authenticate"), contains(basicScheme));
            }
        }
        verify(auditTrail).authenticationFailed(reqId, secondRealm.name(), token, "_action", message);
        verify(auditTrail).authenticationFailed(reqId, token, "_action", message);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRealmAuthenticateThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        doThrow(authenticationError("realm doesn't like authenticate"))
            .when(secondRealm).authenticate(eq(token), any(ActionListener.class));
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        try {
            authenticateBlocking("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like authenticate"));
            verify(auditTrail).authenticationFailed(reqId, token, "_action", message);
        }
    }

    public void testRealmAuthenticateThrowingExceptionRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        doThrow(authenticationError("realm doesn't like authenticate"))
                .when(secondRealm).authenticate(eq(token), any(ActionListener.class));
        try {
            authenticateBlocking(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), is("realm doesn't like authenticate"));
            String reqId = expectAuditRequestId();
            verify(auditTrail).authenticationFailed(reqId, token, restRequest);
        }
    }

    public void testRealmLookupThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doThrow(authenticationError("realm doesn't want to lookup"))
            .when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);

        try {
            authenticateBlocking("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            verify(auditTrail).authenticationFailed(reqId, token, "_action", message);
        }
    }

    public void testRealmLookupThrowingExceptionRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doThrow(authenticationError("realm doesn't want to lookup"))
                .when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));
        try {
            authenticateBlocking(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            String reqId = expectAuditRequestId();
            verify(auditTrail).authenticationFailed(reqId, token, restRequest);
        }
    }

    public void testRunAsLookupSameRealm() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        final User user = new User("lookup user", new String[]{"user"}, "lookup user", "lookup@foo.foo",
                Map.of("foo", "bar"), true);
        mockAuthenticate(secondRealm, token, user);
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doAnswer((i) -> {
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));

        final AtomicBoolean completed = new AtomicBoolean(false);
        ActionListener<Authentication> listener = ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
            User authenticated = result.getUser();

            assertThat(authenticated.principal(), is("looked up user"));
            assertThat(authenticated.roles(), arrayContaining("some role"));
            assertThreadContextContainsAuthentication(result);

            assertThat(SystemUser.is(authenticated), is(false));
            assertThat(authenticated.isRunAs(), is(true));
            User authUser = authenticated.authenticatedUser();
            assertThat(authUser.principal(), is("lookup user"));
            assertThat(authUser.roles(), arrayContaining("user"));
            assertEquals(user.metadata(), authUser.metadata());
            assertEquals(user.email(), authUser.email());
            assertEquals(user.enabled(), authUser.enabled());
            assertEquals(user.fullName(), authUser.fullName());


            setCompletedToTrue(completed);
        }, this::logAndFail);

        // we do not actually go async
        if (randomBoolean()) {
            service.authenticate("_action", message, (User)null, listener);
        } else {
            service.authenticate(restRequest, listener);
        }
        assertTrue(completed.get());
    }

    @SuppressWarnings("unchecked")
    public void testRunAsLookupDifferentRealm() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        doAnswer((i) -> {
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}));
            return null;
        }).when(firstRealm).lookupUser(eq("run_as"), any(ActionListener.class));

        final AtomicBoolean completed = new AtomicBoolean(false);
        ActionListener<Authentication> listener = ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
            User authenticated = result.getUser();

            assertThat(SystemUser.is(authenticated), is(false));
            assertThat(authenticated.isRunAs(), is(true));
            assertThat(authenticated.authenticatedUser().principal(), is("lookup user"));
            assertThat(authenticated.authenticatedUser().roles(), arrayContaining("user"));
            assertThat(authenticated.principal(), is("looked up user"));
            assertThat(authenticated.roles(), arrayContaining("some role"));
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
        }, this::logAndFail);

        // call service asynchronously but it doesn't actually go async
        if (randomBoolean()) {
            service.authenticate("_action", message, (User)null, listener);
        } else {
            service.authenticate(restRequest, listener);
        }
        assertTrue(completed.get());
    }

    public void testRunAsWithEmptyRunAsUsernameRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        User user = new User("lookup user", new String[]{"user"});
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);

        try {
            authenticateBlocking(restRequest);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            String reqId = expectAuditRequestId();
            verify(auditTrail).runAsDenied(eq(reqId), any(Authentication.class), eq(restRequest), eq(EmptyAuthorizationInfo.INSTANCE));
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testRunAsWithEmptyRunAsUsername() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        User user = new User("lookup user", new String[]{"user"});
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "");
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);

        try {
            authenticateBlocking("_action", message, null);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            verify(auditTrail).runAsDenied(eq(reqId), any(Authentication.class), eq("_action"), eq(message),
                eq(EmptyAuthorizationInfo.INSTANCE));
            verifyNoMoreInteractions(auditTrail);
        }
    }

    @SuppressWarnings("unchecked")
    public void testAuthenticateTransportDisabledRunAsUser() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doAnswer((i) -> {
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}, null, null, Map.of(), false));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));
        User fallback = randomBoolean() ? SystemUser.INSTANCE : null;
        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", message, fallback));
        verify(auditTrail).authenticationFailed(reqId, token, "_action", message);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
    }

    public void testAuthenticateRestDisabledRunAsUser() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doAnswer((i) -> {
            @SuppressWarnings("unchecked")
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}, null, null, Map.of(), false));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking(restRequest));
        String reqId = expectAuditRequestId();
        verify(auditTrail).authenticationFailed(reqId, token, restRequest);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
    }

    public void testAuthenticateWithToken() throws Exception {
        User user = new User("_username", "r1");
        final AtomicBoolean completed = new AtomicBoolean(false);
        final Authentication expected = new Authentication(user, new RealmRef("realm", "custom", "node"), null);
        PlainActionFuture<Tuple<String, String>> tokenFuture = new PlainActionFuture<>();
        final String userTokenId = UUIDs.randomBase64UUID();
        final String refreshToken = UUIDs.randomBase64UUID();
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            Authentication originatingAuth = new Authentication(new User("creator"), new RealmRef("test", "test", "test"), null);
            tokenService.createOAuth2Tokens(userTokenId, refreshToken, expected, originatingAuth, Collections.emptyMap(), tokenFuture);
        }
        String token = tokenFuture.get().v1();
        when(client.prepareMultiGet()).thenReturn(new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE));
        mockGetTokenFromId(tokenService, userTokenId, expected, false, client);
        when(securityIndex.isAvailable()).thenReturn(true);
        when(securityIndex.indexExists()).thenReturn(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", "Bearer " + token);
            service.authenticate("_action", message, (User)null, ActionListener.wrap(result -> {
                assertThat(result, notNullValue());
                assertThat(result.getUser(), is(user));
                assertThat(result.getLookedUpBy(), is(nullValue()));
                assertThat(result.getAuthenticatedBy(), is(notNullValue()));
                assertThat(result.getAuthenticationType(), is(AuthenticationType.TOKEN));
                setCompletedToTrue(completed);
            }, this::logAndFail));
        }
        assertTrue(completed.get());
        verify(auditTrail).authenticationSuccess(anyString(), eq("realm"), eq(user), eq("_action"), same(message));
        verifyNoMoreInteractions(auditTrail);
    }

    public void testInvalidToken() throws Exception {
        final User user = new User("_username", "r1");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);
        final int numBytes = randomIntBetween(TokenService.MINIMUM_BYTES, TokenService.MINIMUM_BYTES + 32);
        final byte[] randomBytes = new byte[numBytes];
        random().nextBytes(randomBytes);
        final CountDownLatch latch = new CountDownLatch(1);
        final Authentication expected = new Authentication(user, new RealmRef(firstRealm.name(), firstRealm.type(), "authc_test"), null);
        AtomicBoolean success = new AtomicBoolean(false);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", "Bearer " + Base64.getEncoder().encodeToString(randomBytes));
            service.authenticate("_action", message, (User)null, ActionListener.wrap(result -> {
                assertThat(result, notNullValue());
                assertThat(result.getUser(), is(user));
                assertThat(result.getLookedUpBy(), is(nullValue()));
                assertThat(result.getAuthenticatedBy(), is(notNullValue()));
                assertThreadContextContainsAuthentication(result);
                assertEquals(expected, result);
                success.set(true);
                latch.countDown();
            }, e -> {
                if (e instanceof IllegalStateException) {
                    assertThat(e.getMessage(), containsString("array length must be <= to " + ArrayUtil.MAX_ARRAY_LENGTH  + " but was: "));
                    latch.countDown();
                } else if (e instanceof NegativeArraySizeException) {
                    assertThat(e.getMessage(), containsString("array size must be positive but was: "));
                    latch.countDown();
                } else {
                    logger.error("unexpected exception", e);
                    latch.countDown();
                    fail("unexpected exception: " + e.getMessage());
                }
            }));
        } catch (IllegalStateException ex) {
            assertThat(ex.getMessage(), containsString("array length must be <= to " + ArrayUtil.MAX_ARRAY_LENGTH  + " but was: "));
            latch.countDown();
        } catch (NegativeArraySizeException ex) {
            assertThat(ex.getMessage(), containsString("array size must be positive but was: "));
            latch.countDown();
        }

        // we need to use a latch here because the key computation goes async on another thread!
        latch.await();
        if (success.get()) {
            final String realmName = firstRealm.name();
            verify(auditTrail).authenticationSuccess(anyString(), eq(realmName), eq(user), eq("_action"), same(message));
        }
        verifyNoMoreInteractions(auditTrail);
    }

    public void testExpiredToken() throws Exception {
        when(securityIndex.isAvailable()).thenReturn(true);
        when(securityIndex.indexExists()).thenReturn(true);
        User user = new User("_username", "r1");
        final Authentication expected = new Authentication(user, new RealmRef("realm", "custom", "node"), null);
        PlainActionFuture<Tuple<String, String>> tokenFuture = new PlainActionFuture<>();
        final String userTokenId = UUIDs.randomBase64UUID();
        final String refreshToken = UUIDs.randomBase64UUID();
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            Authentication originatingAuth = new Authentication(new User("creator"), new RealmRef("test", "test", "test"), null);
            tokenService.createOAuth2Tokens(userTokenId, refreshToken, expected, originatingAuth, Collections.emptyMap(), tokenFuture);
        }
        String token = tokenFuture.get().v1();
        mockGetTokenFromId(tokenService, userTokenId, expected, true, client);
        doAnswer(invocationOnMock -> {
            ((Runnable) invocationOnMock.getArguments()[1]).run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(any(Consumer.class), any(Runnable.class));

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", "Bearer " + token);
            ElasticsearchSecurityException e =
                    expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", message, null));
            assertEquals(RestStatus.UNAUTHORIZED, e.status());
            assertEquals("token expired", e.getMessage());
        }
    }

    public void testApiKeyAuthInvalidHeader() {
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final String invalidHeader = randomFrom("apikey", "apikey ", "apikey foo");
            threadContext.putHeader("Authorization", invalidHeader);
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> authenticateBlocking("_action", message, null));
            assertEquals(RestStatus.UNAUTHORIZED, e.status());
            assertThat(e.getMessage(), containsString("missing authentication credentials"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testApiKeyAuth() {
        final String id = randomAlphaOfLength(12);
        final String key = UUIDs.randomBase64UUID(random());
        final String headerValue = "ApiKey " + Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
        doAnswer(invocationOnMock -> {
            final GetRequest request = (GetRequest) invocationOnMock.getArguments()[0];
            final ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
            if (request.id().equals(id)) {
                final Map<String, Object> source = new HashMap<>();
                source.put("doc_type", "api_key");
                source.put("creation_time", Instant.now().minus(5, ChronoUnit.MINUTES).toEpochMilli());
                source.put("api_key_invalidated", false);
                source.put("api_key_hash", new String(Hasher.BCRYPT4.hash(new SecureString(key.toCharArray()))));
                source.put("role_descriptors", Collections.singletonMap("api key role", Collections.singletonMap("cluster", "all")));
                source.put("name", "my api key for testApiKeyAuth");
                Map<String, Object> creatorMap = new HashMap<>();
                creatorMap.put("principal", "johndoe");
                creatorMap.put("metadata", Collections.emptyMap());
                creatorMap.put("realm", "auth realm");
                source.put("creator", creatorMap);
                GetResponse getResponse = new GetResponse(new GetResult(request.index(), request.id(), 0, 1, 1L, true,
                    BytesReference.bytes(JsonXContent.contentBuilder().map(source)), Collections.emptyMap(), Collections.emptyMap()));
                listener.onResponse(getResponse);
            } else {
                listener.onResponse(new GetResponse(new GetResult(request.index(), request.id(),
                        SequenceNumbers.UNASSIGNED_SEQ_NO, 1, -1L, false, null,
                    Collections.emptyMap(), Collections.emptyMap())));
            }
            return Void.TYPE;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", headerValue);
            final Authentication authentication = authenticateBlocking("_action", message, null);
            assertThat(authentication.getUser().principal(), is("johndoe"));
            assertThat(authentication.getAuthenticationType(), is(AuthenticationType.API_KEY));
        }
    }

    public void testExpiredApiKey() {
        final String id = randomAlphaOfLength(12);
        final String key = UUIDs.randomBase64UUID(random());
        final String headerValue = "ApiKey " + Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
        doAnswer(invocationOnMock -> {
            final GetRequest request = (GetRequest) invocationOnMock.getArguments()[0];
            final ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
            if (request.id().equals(id)) {
                final Map<String, Object> source = new HashMap<>();
                source.put("doc_type", "api_key");
                source.put("creation_time", Instant.now().minus(5L, ChronoUnit.HOURS).toEpochMilli());
                source.put("expiration_time", Instant.now().minus(1L, ChronoUnit.HOURS).toEpochMilli());
                source.put("api_key_invalidated", false);
                source.put("api_key_hash", new String(Hasher.BCRYPT4.hash(new SecureString(key.toCharArray()))));
                source.put("role_descriptors", Collections.singletonList(Collections.singletonMap("name", "a role")));
                source.put("name", "my api key for testApiKeyAuth");
                Map<String, Object> creatorMap = new HashMap<>();
                creatorMap.put("principal", "johndoe");
                creatorMap.put("metadata", Collections.emptyMap());
                creatorMap.put("realm", "auth realm");
                source.put("creator", creatorMap);
                GetResponse getResponse = new GetResponse(new GetResult(request.index(), request.id(), 0, 1, 1L, true,
                        BytesReference.bytes(JsonXContent.contentBuilder().map(source)), Collections.emptyMap(), Collections.emptyMap()));
                listener.onResponse(getResponse);
            } else {
                listener.onResponse(new GetResponse(new GetResult(request.index(), request.id(),
                        SequenceNumbers.UNASSIGNED_SEQ_NO, 1, -1L, false, null,
                    Collections.emptyMap(), Collections.emptyMap())));
            }
            return Void.TYPE;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", headerValue);
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> authenticateBlocking("_action", message, null));
            assertEquals(RestStatus.UNAUTHORIZED, e.status());
        }
    }

    private static class InternalMessage extends TransportMessage {
        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    void assertThreadContextContainsAuthentication(Authentication authentication) throws IOException {
        Authentication contextAuth = threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY);
        assertThat(contextAuth, notNullValue());
        assertThat(contextAuth, is(authentication));
        assertThat(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY), equalTo((Object) authentication.encode()));
    }

    @SuppressWarnings("unchecked")
    private void mockAuthenticate(Realm realm, AuthenticationToken token, User user) {
        final boolean separateThread = randomBoolean();
        doAnswer(i -> {
            ActionListener<AuthenticationResult> listener = (ActionListener<AuthenticationResult>) i.getArguments()[1];
            Runnable run = () -> {
                if (user == null) {
                    listener.onResponse(AuthenticationResult.notHandled());
                } else {
                    listener.onResponse(AuthenticationResult.success(user));
                }
            };
            if (separateThread) {
                final Thread thread = new Thread(run);
                thread.start();
                thread.join();
            } else {
                run.run();
            }
            return null;
        }).when(realm).authenticate(eq(token), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    private void mockAuthenticate(Realm realm, AuthenticationToken token, Exception e, boolean terminate) {
        doAnswer((i) -> {
            ActionListener<AuthenticationResult> listener = (ActionListener<AuthenticationResult>) i.getArguments()[1];
            if (terminate) {
                listener.onResponse(AuthenticationResult.terminate("terminate authc process", e));
            } else {
                listener.onResponse(AuthenticationResult.unsuccessful("unsuccessful, but continue authc process", e));
            }
            return null;
        }).when(realm).authenticate(eq(token), any(ActionListener.class));
    }

    private Authentication authenticateBlocking(RestRequest restRequest) {
        PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticate(restRequest, future);
        return future.actionGet();
    }

    private Authentication authenticateBlocking(String action, TransportMessage message, User fallbackUser) {
        PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticate(action, message, fallbackUser, future);
        return future.actionGet();
    }

    private String expectAuditRequestId() {
        String reqId = AuditUtil.extractRequestId(threadContext);
        assertThat(reqId, not(isEmptyOrNullString()));
        return reqId;
    }

    @SuppressWarnings("unchecked")
    private static void mockRealmLookupReturnsNull(Realm realm, String username) {
        doAnswer((i) -> {
            ActionListener<?> listener = (ActionListener<?>) i.getArguments()[1];
            listener.onResponse(null);
            return null;
        }).when(realm).lookupUser(eq(username), any(ActionListener.class));
    }

    static class TestRealms extends Realms {

        TestRealms(Settings settings, Environment env, Map<String, Factory> factories, XPackLicenseState licenseState,
                   ThreadContext threadContext, ReservedRealm reservedRealm, List<Realm> realms, List<Realm> internalRealms)
                throws Exception {
            super(settings, env, factories, licenseState, threadContext, reservedRealm);
            this.realms = realms;
            this.standardRealmsOnly = internalRealms;
        }
    }

    private void logAndFail(Exception e) {
        logger.error("unexpected exception", e);
        fail("unexpected exception " + e.getMessage());
    }

    private void setCompletedToTrue(AtomicBoolean completed) {
        assertTrue(completed.compareAndSet(false, true));
    }

    private SecurityIndexManager.State dummyState(ClusterHealthStatus indexStatus) {
        return new SecurityIndexManager.State(
            Instant.now(), true, true, true, null, concreteSecurityIndexName, indexStatus, IndexMetaData.State.OPEN);
    }
}
