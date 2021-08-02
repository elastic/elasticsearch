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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
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
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
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
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.EmptyAuthorizationInfo;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.core.security.support.ValidationTests;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.AuthenticationService.Authenticator;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;
import static org.elasticsearch.xpack.security.Security.SECURITY_CRYPTO_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.security.authc.TokenService.THREAD_POOL_NAME;
import static org.elasticsearch.xpack.security.authc.TokenServiceTests.mockGetTokenFromId;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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
    private TransportRequest transportRequest;
    private RestRequest restRequest;
    private Realms realms;
    private Realm firstRealm;
    private Realm secondRealm;
    private AuditTrail auditTrail;
    private AuditTrailService auditTrailService;
    private AuthenticationToken token;
    private ThreadPool threadPool;
    private ThreadContext threadContext;
    private TokenService tokenService;
    private ApiKeyService apiKeyService;
    private ServiceAccountService serviceAccountService;
    private SecurityIndexManager securityIndex;
    private Client client;
    private InetSocketAddress remoteAddress;
    private OperatorPrivileges.OperatorPrivilegesService operatorPrivilegesService;
    private String concreteSecurityIndexName;

    @Before
    @SuppressForbidden(reason = "Allow accessing localhost")
    public void init() throws Exception {
        concreteSecurityIndexName = randomFrom(
            RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_6, RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7);

        token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        transportRequest = new InternalRequest();
        remoteAddress = new InetSocketAddress(InetAddress.getLocalHost(), 100);
        transportRequest.remoteAddress(new TransportAddress(remoteAddress));
        restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withRemoteAddress(remoteAddress).build();
        threadContext = new ThreadContext(Settings.EMPTY);

        firstRealm = mock(Realm.class);
        when(firstRealm.type()).thenReturn(FIRST_REALM_TYPE);
        when(firstRealm.name()).thenReturn(FIRST_REALM_NAME);
        when(firstRealm.toString()).thenReturn(FIRST_REALM_NAME + "/" + FIRST_REALM_TYPE);
        secondRealm = mock(Realm.class);
        when(secondRealm.type()).thenReturn(SECOND_REALM_TYPE);
        when(secondRealm.name()).thenReturn(SECOND_REALM_NAME);
        when(secondRealm.toString()).thenReturn(SECOND_REALM_NAME + "/" + SECOND_REALM_TYPE);
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("node.name", "authc_test")
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true)
            .build();
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.checkFeature(Feature.SECURITY_ALL_REALMS)).thenReturn(true);
        when(licenseState.checkFeature(Feature.SECURITY_TOKEN_SERVICE)).thenReturn(true);
        when(licenseState.copyCurrentLicenseState()).thenReturn(licenseState);
        when(licenseState.checkFeature(Feature.SECURITY_AUDITING)).thenReturn(true);
        ReservedRealm reservedRealm = mock(ReservedRealm.class);
        when(reservedRealm.type()).thenReturn("reserved");
        when(reservedRealm.name()).thenReturn("reserved_realm");
        realms = spy(new TestRealms(Settings.EMPTY, TestEnvironment.newEnvironment(settings),
            Map.of(FileRealmSettings.TYPE, config -> mock(FileRealm.class), NativeRealmSettings.TYPE, config -> mock(NativeRealm.class)),
            licenseState, threadContext, reservedRealm, Arrays.asList(firstRealm, secondRealm),
            Collections.singletonList(firstRealm)));

        auditTrail = mock(AuditTrail.class);
        auditTrailService = new AuditTrailService(Collections.singletonList(auditTrail), licenseState, settings);
        client = mock(Client.class);
        threadPool = new ThreadPool(settings,
            new FixedExecutorBuilder(settings, THREAD_POOL_NAME, 1, 1000,
                "xpack.security.authc.token.thread_pool", false),
            new FixedExecutorBuilder(Settings.EMPTY, SECURITY_CRYPTO_THREAD_POOL_NAME, 1, 1000,
                "xpack.security.crypto.thread_pool", false)
        );
        threadContext = threadPool.getThreadContext();
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        when(client.prepareIndex(any(String.class)))
            .thenReturn(new IndexRequestBuilder(client, IndexAction.INSTANCE));
        when(client.prepareUpdate(any(String.class), any(String.class)))
            .thenReturn(new UpdateRequestBuilder(client, UpdateAction.INSTANCE));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndexResponse> responseActionListener = (ActionListener<IndexResponse>) invocationOnMock.getArguments()[2];
            responseActionListener.onResponse(new IndexResponse(new ShardId(".security", UUIDs.randomBase64UUID(), randomInt()),
                    randomAlphaOfLength(4), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), true));
            return null;
        }).when(client).execute(eq(IndexAction.INSTANCE), any(IndexRequest.class), anyActionListener());
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
        }).when(securityIndex).prepareIndexIfNeededThenExecute(anyConsumer(), any(Runnable.class));
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(securityIndex).checkIndexVersionThenExecute(anyConsumer(), any(Runnable.class));
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        final SecurityContext securityContext = new SecurityContext(settings, threadContext);
        apiKeyService = new ApiKeyService(settings, Clock.systemUTC(), client, securityIndex, clusterService,
                                          mock(CacheInvalidatorRegistry.class), threadPool);
        tokenService = new TokenService(settings, Clock.systemUTC(), client, licenseState, securityContext, securityIndex, securityIndex,
            clusterService);
        serviceAccountService = mock(ServiceAccountService.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Authentication> listener = (ActionListener<Authentication>) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return null;
        }).when(serviceAccountService).authenticateToken(any(), any(), any());

        operatorPrivilegesService = mock(OperatorPrivileges.OperatorPrivilegesService.class);
        service = new AuthenticationService(settings, realms, auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool, new AnonymousUser(settings), tokenService, apiKeyService, serviceAccountService,
            operatorPrivilegesService);
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
        Authenticator authenticator = service.createAuthenticator("_action", transportRequest, true, future);
        AuthenticationToken result = authenticator.extractToken();
        assertThat(result, notNullValue());
        assertThat(result, is(token));
        verifyZeroInteractions(auditTrail);
    }

    public void testTokenMissing() throws Exception {
        final Logger unlicensedRealmsLogger = LogManager.getLogger(AuthenticationService.class);
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        try {
            Loggers.addAppender(unlicensedRealmsLogger, mockAppender);
            mockAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "unlicensed realms",
                AuthenticationService.class.getName(), Level.WARN,
                "No authentication credential could be extracted using realms [file_realm/file]. " +
                    "Realms [second_realm/second] were skipped because they are not permitted on the current license"
            ));

            Mockito.doReturn(List.of(secondRealm)).when(realms).getUnlicensedRealms();
            Mockito.doReturn(List.of(firstRealm)).when(realms).asList();
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            PlainActionFuture<Authentication> future = new PlainActionFuture<>();
            Authenticator authenticator = service.createAuthenticator("_action", transportRequest, true, future);
            AuthenticationToken token = authenticator.extractToken();
            assertThat(token, nullValue());
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            authenticator.handleNullToken();

            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> future.actionGet());
            assertThat(e.getMessage(), containsString("missing authentication credentials"));
            verify(auditTrail).anonymousAccessDenied(reqId.get(), "_action", transportRequest);
            verifyNoMoreInteractions(auditTrail);
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(unlicensedRealmsLogger, mockAppender);
            mockAppender.stop();
        }
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
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }

        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate("_action", transportRequest, true, ActionListener.wrap(result -> {
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getLookedUpBy(), is(nullValue()));
            assertThat(result.getAuthenticatedBy(), is(notNullValue())); // TODO implement equals
            assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
        }, this::logAndFail));
        assertTrue(completed.get());
        verify(auditTrail).authenticationFailed(reqId.get(), firstRealm.name(), token, "_action", transportRequest);
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
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }

        // Authenticate against the normal chain. 1st Realm will be checked (and not pass) then 2nd realm will successfully authc
        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate("_action", transportRequest, true, ActionListener.wrap(result -> {
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getLookedUpBy(), is(nullValue()));
            assertThat(result.getAuthenticatedBy(), is(notNullValue())); // TODO implement equals
            assertThat(result.getAuthenticatedBy().getName(), is(SECOND_REALM_NAME));
            assertThat(result.getAuthenticatedBy().getType(), is(SECOND_REALM_TYPE));
            assertThreadContextContainsAuthentication(result);
            verify(auditTrail).authenticationSuccess(reqId.get(), result, "_action", transportRequest);
            setCompletedToTrue(completed);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
        }, this::logAndFail));
        assertTrue(completed.get());

        completed.set(false);
        // Authenticate against the smart chain.
        // "SecondRealm" will be at the top of the list and will successfully authc.
        // "FirstRealm" will not be used
        Mockito.reset(operatorPrivilegesService);
        service.authenticate("_action", transportRequest, true, ActionListener.wrap(result -> {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getLookedUpBy(), is(nullValue()));
            assertThat(result.getAuthenticatedBy(), is(notNullValue())); // TODO implement equals
            assertThat(result.getAuthenticatedBy().getName(), is(SECOND_REALM_NAME));
            assertThat(result.getAuthenticatedBy().getType(), is(SECOND_REALM_TYPE));
            assertThreadContextContainsAuthentication(result);
            verify(auditTrail, times(2)).authenticationSuccess(reqId.get(), result, "_action", transportRequest);
            setCompletedToTrue(completed);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
        }, this::logAndFail));

        verify(auditTrail).authenticationFailed(reqId.get(), firstRealm.name(), token, "_action", transportRequest);
        verify(firstRealm, times(2)).name(); // used above one time
        verify(secondRealm, times(2)).name();
        verify(secondRealm, times(2)).type(); // used to create realm ref
        verify(firstRealm, times(2)).token(threadContext);
        verify(secondRealm, times(2)).token(threadContext);
        verify(firstRealm).supports(token);
        verify(secondRealm, times(2)).supports(token);
        verify(firstRealm).authenticate(eq(token), anyActionListener());
        verify(secondRealm, times(2)).authenticate(eq(token), anyActionListener());
        verifyNoMoreInteractions(auditTrail, firstRealm, secondRealm);

        // Now assume some change in the backend system so that 2nd realm no longer has the user, but the 1st realm does.
        mockAuthenticate(secondRealm, token, null);
        mockAuthenticate(firstRealm, token, user);

        completed.set(false);
        // This will authenticate against the smart chain.
        // "SecondRealm" will be at the top of the list but will no longer authenticate the user.
        // Then "FirstRealm" will be checked.
        service.authenticate("_action", transportRequest, true, ActionListener.wrap(result -> {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getLookedUpBy(), is(nullValue()));
            assertThat(result.getAuthenticatedBy(), is(notNullValue()));
            assertThat(result.getAuthenticatedBy().getName(), is(FIRST_REALM_NAME));
            assertThat(result.getAuthenticatedBy().getType(), is(FIRST_REALM_TYPE));
            assertThreadContextContainsAuthentication(result);
            verify(auditTrail).authenticationSuccess(reqId.get(), result, "_action", transportRequest);
            setCompletedToTrue(completed);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
        }, this::logAndFail));

        verify(auditTrail).authenticationFailed(reqId.get(), SECOND_REALM_NAME, token, "_action", transportRequest);
        verify(secondRealm, times(3)).authenticate(eq(token), anyActionListener()); // 2 from above + 1 more
        verify(firstRealm, times(2)).authenticate(eq(token), anyActionListener()); // 1 from above + 1 more
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
        service = new AuthenticationService(settings, realms, auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool, new AnonymousUser(Settings.EMPTY),
            tokenService, apiKeyService, serviceAccountService, operatorPrivilegesService);
        User user = new User("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, null);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);
        when(secondRealm.token(threadContext)).thenReturn(token);
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }

        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate("_action", transportRequest, true, ActionListener.wrap(result -> {
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getLookedUpBy(), is(nullValue()));
            assertThat(result.getAuthenticatedBy().getName(), is(SECOND_REALM_NAME)); // TODO implement equals
            assertThreadContextContainsAuthentication(result);
            verify(auditTrail).authenticationSuccess(reqId.get(), result, "_action", transportRequest);
            setCompletedToTrue(completed);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
        }, this::logAndFail));
        assertTrue(completed.get());

        completed.set(false);
        Mockito.reset(operatorPrivilegesService);
        service.authenticate("_action", transportRequest, true, ActionListener.wrap(result -> {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getLookedUpBy(), is(nullValue()));
            assertThat(result.getAuthenticatedBy().getName(), is(SECOND_REALM_NAME)); // TODO implement equals
            assertThreadContextContainsAuthentication(result);
            verify(auditTrail, times(2)).authenticationSuccess(reqId.get(), result, "_action", transportRequest);
            setCompletedToTrue(completed);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
        }, this::logAndFail));
        verify(auditTrail, times(2)).authenticationFailed(reqId.get(), firstRealm.name(), token, "_action", transportRequest);
        verify(firstRealm, times(3)).name(); // used above one time
        verify(secondRealm, times(2)).name();
        verify(secondRealm, times(2)).type(); // used to create realm ref
        verify(firstRealm, times(2)).token(threadContext);
        verify(secondRealm, times(2)).token(threadContext);
        verify(firstRealm, times(2)).supports(token);
        verify(secondRealm, times(2)).supports(token);
        verify(firstRealm, times(2)).authenticate(eq(token), anyActionListener());
        verify(secondRealm, times(2)).authenticate(eq(token), anyActionListener());
        verifyNoMoreInteractions(auditTrail, firstRealm, secondRealm);
    }

    public void testAuthenticateFirstNotSupportingSecondSucceeds() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(false);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);
        when(secondRealm.token(threadContext)).thenReturn(token);
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }

        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate("_action", transportRequest, true, ActionListener.wrap(result -> {
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
            assertThat(result.getAuthenticatedBy().getName(), is(secondRealm.name())); // TODO implement equals
            assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
            assertThreadContextContainsAuthentication(result);
            verify(auditTrail).authenticationSuccess(reqId.get(), result, "_action", transportRequest);
            setCompletedToTrue(completed);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
        }, this::logAndFail));
        verifyNoMoreInteractions(auditTrail);
        verify(firstRealm, never()).authenticate(eq(token), anyActionListener());
        assertTrue(completed.get());
    }

    public void testAuthenticateCached() throws Exception {
        final Authentication authentication = new Authentication(new User("_username", "r1"), new RealmRef("test", "cached", "foo"), null);
        authentication.writeToContext(threadContext);
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }

        Tuple<Authentication, String> result = authenticateBlocking("_action", transportRequest, null);

        if (requestIdAlreadyPresent) {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        }
        assertThat(expectAuditRequestId(threadContext), is(result.v2()));
        assertThat(result, notNullValue());
        assertThat(result.v1(), is(authentication));
        assertThat(result.v1().getAuthenticationType(), is(AuthenticationType.REALM));
        verifyZeroInteractions(auditTrail);
        verifyZeroInteractions(firstRealm);
        verifyZeroInteractions(secondRealm);
        verifyZeroInteractions(operatorPrivilegesService);
    }

    public void testAuthenticateNonExistentRestRequestUserThrowsAuthenticationException() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(new UsernamePasswordToken("idonotexist",
                new SecureString("passwd".toCharArray())));
        try {
            authenticateBlocking(restRequest);
            fail("Authentication was successful but should not");
        } catch (ElasticsearchSecurityException e) {
            expectAuditRequestId(threadContext);
            assertAuthenticationException(e, containsString("unable to authenticate user [idonotexist] for REST request [/]"));
            verifyZeroInteractions(operatorPrivilegesService);
        }
    }

    public void testTokenRestMissing() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);

        @SuppressWarnings("unchecked")
        Authenticator authenticator = service.createAuthenticator(restRequest, true, mock(ActionListener.class));
        AuthenticationToken token = authenticator.extractToken();
        expectAuditRequestId(threadContext);
        assertThat(token, nullValue());
    }

    public void testAuthenticationInContextAndHeader() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }

        service.authenticate("_action", transportRequest, true, ActionListener.wrap(result -> {
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            }
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));

            String userStr = threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY);
            assertThat(userStr, notNullValue());
            Authentication ctxAuth = threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY);
            assertThat(ctxAuth, is(result));
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
        }, this::logAndFail));
    }

    public void testAuthenticateTransportAnonymous() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        try {
            authenticateBlocking("_action", transportRequest, null);
            fail("expected an authentication exception when trying to authenticate an anonymous message");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthenticationException(e);
            verifyZeroInteractions(operatorPrivilegesService);
        }
        if (requestIdAlreadyPresent) {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        } else {
            reqId.set(expectAuditRequestId(threadContext));
        }
        verify(auditTrail).anonymousAccessDenied(reqId.get(), "_action", transportRequest);
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
            verifyZeroInteractions(operatorPrivilegesService);
        }
        verify(auditTrail).anonymousAccessDenied(expectAuditRequestId(threadContext), restRequest);
    }

    public void testAuthenticateTransportFallback() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);
        User user1 = new User("username", "r1", "r2");
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }

        Tuple<Authentication, String> result = authenticateBlocking("_action", transportRequest, user1);
        if (requestIdAlreadyPresent) {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        } else {
            reqId.set(expectAuditRequestId(threadContext));
        }
        assertThat(expectAuditRequestId(threadContext), is(result.v2()));
        assertThat(result, notNullValue());
        assertThat(result.v1().getUser(), sameInstance(user1));
        assertThat(result.v1().getAuthenticationType(), is(AuthenticationType.INTERNAL));
        assertThreadContextContainsAuthentication(result.v1());
        verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result.v1()), eq(threadContext));
    }

    public void testAuthenticateTransportDisabledUser() throws Exception {
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        User user = new User("username", new String[] { "r1", "r2" }, null, null, Map.of(), false);
        User fallback = randomBoolean() ? SystemUser.INSTANCE : null;
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", transportRequest, fallback));
        if (requestIdAlreadyPresent) {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        } else {
            reqId.set(expectAuditRequestId(threadContext));
        }
        verify(auditTrail).authenticationFailed(reqId.get(), token, "_action", transportRequest);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
        verifyZeroInteractions(operatorPrivilegesService);
    }

    public void testAuthenticateRestDisabledUser() throws Exception {
        User user = new User("username", new String[] { "r1", "r2" }, null, null, Map.of(), false);
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking(restRequest));
        String reqId = expectAuditRequestId(threadContext);
        verify(auditTrail).authenticationFailed(reqId, token, restRequest);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
        verifyZeroInteractions(operatorPrivilegesService);
    }

    public void testAuthenticateTransportSuccess() throws Exception {
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        final User user = new User("username", "r1", "r2");
        final Consumer<ActionListener<Authentication>> authenticate;
        if (randomBoolean()) {
            authenticate = listener -> service.authenticate("_action", transportRequest, SystemUser.INSTANCE, listener);
        } else {
            authenticate = listener -> service.authenticate("_action", transportRequest, true, listener);
        }
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        final AtomicBoolean completed = new AtomicBoolean(false);
        authenticate.accept(ActionListener.wrap(result -> {
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            assertThat(result, notNullValue());
            assertThat(result.getUser(), sameInstance(user));
            assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
            assertThat(result.getAuthenticatedBy().getName(), is(firstRealm.name())); // TODO implement equals
            assertThreadContextContainsAuthentication(result);
            verify(auditTrail).authenticationSuccess(reqId.get(), result, "_action", transportRequest);
            setCompletedToTrue(completed);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
        }, this::logAndFail));

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
            assertThat(authentication.getAuthenticatedBy().getName(), is(firstRealm.name())); // TODO implement equals
            assertThreadContextContainsAuthentication(authentication);
            String reqId = expectAuditRequestId(threadContext);
            verify(auditTrail).authenticationSuccess(reqId, authentication, restRequest);
            setCompletedToTrue(completed);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(authentication), eq(threadContext));
        }, this::logAndFail));
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
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            service.authenticate("_action", transportRequest, SystemUser.INSTANCE, ActionListener.wrap(authentication -> {
                    if (requestIdAlreadyPresent) {
                        assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
                    } else {
                        reqId.set(expectAuditRequestId(threadContext));
                    }
                    assertThat(authentication, notNullValue());
                    assertThat(authentication.getUser(), sameInstance(user1));
                    assertThat(authentication.getAuthenticationType(), is(AuthenticationType.REALM));
                    assertThreadContextContainsAuthentication(authentication);
                    authRef.set(authentication);
                    authHeaderRef.set(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY));
                    setCompletedToTrue(completed);
                    verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(authentication), eq(threadContext));
                }, this::logAndFail));
        }
        assertTrue(completed.compareAndSet(true, false));
        reset(firstRealm);

        // checking authentication from the context
        InternalRequest message1 = new InternalRequest();
        ThreadPool threadPool1 = new TestThreadPool("testAutheticateTransportContextAndHeader1");
        Mockito.reset(operatorPrivilegesService);
        try {
            ThreadContext threadContext1 = threadPool1.getThreadContext();
            service = new AuthenticationService(Settings.EMPTY, realms, auditTrailService,
                new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool1, new AnonymousUser(Settings.EMPTY),
                tokenService, apiKeyService, serviceAccountService, operatorPrivilegesService);
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext1));
            }

            threadContext1.putTransient(AuthenticationField.AUTHENTICATION_KEY, authRef.get());
            threadContext1.putHeader(AuthenticationField.AUTHENTICATION_KEY, authHeaderRef.get());
            service.authenticate("_action", message1, SystemUser.INSTANCE, ActionListener.wrap(ctxAuth -> {
                    if (requestIdAlreadyPresent) {
                        assertThat(expectAuditRequestId(threadContext1), is(reqId.get()));
                    } else {
                        reqId.set(expectAuditRequestId(threadContext1));
                    }
                    assertThat(ctxAuth, sameInstance(authRef.get()));
                    assertThat(threadContext1.getHeader(AuthenticationField.AUTHENTICATION_KEY), sameInstance(authHeaderRef.get()));
                    setCompletedToTrue(completed);
                    verifyZeroInteractions(operatorPrivilegesService);
                }, this::logAndFail));
            assertTrue(completed.compareAndSet(true, false));
            verifyZeroInteractions(firstRealm);
            reset(firstRealm);
        } finally {
            terminate(threadPool1);
        }

        // checking authentication from the user header
        ThreadPool threadPool2 = new TestThreadPool("testAutheticateTransportContextAndHeader2");
        Mockito.reset(operatorPrivilegesService);
        try {
            ThreadContext threadContext2 = threadPool2.getThreadContext();
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext2));
            }
            final String header;
            try (ThreadContext.StoredContext ignore = threadContext2.stashContext()) {
                service = new AuthenticationService(Settings.EMPTY, realms, auditTrailService,
                    new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool2, new AnonymousUser(Settings.EMPTY),
                    tokenService, apiKeyService, serviceAccountService, operatorPrivilegesService);
                threadContext2.putHeader(AuthenticationField.AUTHENTICATION_KEY, authHeaderRef.get());

                BytesStreamOutput output = new BytesStreamOutput();
                threadContext2.writeTo(output);
                StreamInput input = output.bytes().streamInput();
                threadContext2 = new ThreadContext(Settings.EMPTY);
                threadContext2.readHeaders(input);
                header = threadContext2.getHeader(AuthenticationField.AUTHENTICATION_KEY);
            }

            threadPool2.getThreadContext().putHeader(AuthenticationField.AUTHENTICATION_KEY, header);
            service = new AuthenticationService(Settings.EMPTY, realms, auditTrailService,
                new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool2, new AnonymousUser(Settings.EMPTY),
                tokenService, apiKeyService, serviceAccountService, operatorPrivilegesService);
            service.authenticate("_action", new InternalRequest(), SystemUser.INSTANCE, ActionListener.wrap(result -> {
                    if (requestIdAlreadyPresent) {
                        assertThat(expectAuditRequestId(threadPool2.getThreadContext()), is(reqId.get()));
                    } else {
                        reqId.set(expectAuditRequestId(threadPool2.getThreadContext()));
                    }
                    assertThat(result, notNullValue());
                    assertThat(result.getUser(), equalTo(user1));
                    assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
                    setCompletedToTrue(completed);
                    verifyZeroInteractions(operatorPrivilegesService);
                }, this::logAndFail));
            assertTrue(completed.get());
            verifyZeroInteractions(firstRealm);
        } finally {
            terminate(threadPool2);
        }
    }

    public void testAuthenticateTamperedUser() throws Exception {
        InternalRequest message = new InternalRequest();
        threadContext.putHeader(AuthenticationField.AUTHENTICATION_KEY, "_signed_auth");
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        try {
            authenticateBlocking("_action", message, randomBoolean() ? SystemUser.INSTANCE : null);
        } catch (Exception e) {
            //expected
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).tamperedRequest(reqId.get(), "_action", message);
            verifyNoMoreInteractions(auditTrail);
            verifyZeroInteractions(operatorPrivilegesService);
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
        service = new AuthenticationService(anonymousEnabledSettings, realms, auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool, anonymousUser,
            tokenService, apiKeyService, serviceAccountService, operatorPrivilegesService);

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            threadContext.putHeader("Authorization", "Bearer thisisaninvalidtoken");
            ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", transportRequest, null));
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).anonymousAccessDenied(reqId.get(), "_action", transportRequest);
            verifyNoMoreInteractions(auditTrail);
            assertAuthenticationException(e);
            verifyZeroInteractions(operatorPrivilegesService);
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
        service = new AuthenticationService(anonymousEnabledSettings, realms, auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool, anonymousUser,
            tokenService, apiKeyService, serviceAccountService, operatorPrivilegesService);
        doAnswer(invocationOnMock -> {
            final GetRequest request = (GetRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            final ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(new GetResponse(new GetResult(request.index(), request.id(),
                SequenceNumbers.UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, -1L, false, null,
                Collections.emptyMap(), Collections.emptyMap())));
            return Void.TYPE;
        }).when(client).get(any(GetRequest.class), anyActionListener());
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            threadContext.putHeader("Authorization", "ApiKey dGhpc2lzYW5pbnZhbGlkaWQ6dGhpc2lzYW5pbnZhbGlkc2VjcmV0");
            ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", transportRequest, null));
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).anonymousAccessDenied(reqId.get(), "_action", transportRequest);
            verifyNoMoreInteractions(auditTrail);
            assertAuthenticationException(e);
            verifyZeroInteractions(operatorPrivilegesService);
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
        service = new AuthenticationService(settings, realms, auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool, anonymousUser, tokenService, apiKeyService, serviceAccountService, operatorPrivilegesService);
        RestRequest request = new FakeRestRequest();

        Tuple<Authentication, String> result = authenticateBlocking(request);

        assertThat(result, notNullValue());
        assertThat(result.v1().getUser(), sameInstance((Object) anonymousUser));
        assertThat(result.v1().getAuthenticationType(), is(AuthenticationType.ANONYMOUS));
        assertThreadContextContainsAuthentication(result.v1());
        assertThat(expectAuditRequestId(threadContext), is(result.v2()));
        verify(auditTrail).authenticationSuccess(result.v2(), result.v1(), request);
        verifyNoMoreInteractions(auditTrail);
        verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result.v1()), eq(threadContext));
    }

    public void testAuthenticateRestRequestDisallowAnonymous() throws Exception {
        final String username = randomBoolean() ? AnonymousUser.DEFAULT_ANONYMOUS_USERNAME : "_anon_" + randomAlphaOfLengthBetween(2, 6);
        final Settings.Builder builder = Settings.builder()
            .putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3");
        if (username.equals(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME) == false) {
            builder.put(AnonymousUser.USERNAME_SETTING.getKey(), username);
        }
        Settings settings = builder.build();

        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(settings, realms, auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool, anonymousUser, tokenService, apiKeyService, serviceAccountService, operatorPrivilegesService);
        RestRequest request = new FakeRestRequest();

        PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticate(request, false, future);
        final ElasticsearchSecurityException ex = expectThrows(ElasticsearchSecurityException.class, future::actionGet);

        assertThat(ex, notNullValue());
        assertThat(ex, throwableWithMessage(containsString("missing authentication credentials for REST request")));
        assertThat(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY), nullValue());
        assertThat(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY), nullValue());
        String reqId = expectAuditRequestId(threadContext);
        verify(auditTrail).anonymousAccessDenied(reqId, request);
        verifyNoMoreInteractions(auditTrail);
        verifyZeroInteractions(operatorPrivilegesService);
    }

    public void testAnonymousUserTransportNoDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(settings, realms, auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool, anonymousUser, tokenService, apiKeyService, serviceAccountService, operatorPrivilegesService);
        InternalRequest message = new InternalRequest();
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }

        Tuple<Authentication, String> result = authenticateBlocking("_action", message, null);
        if (requestIdAlreadyPresent) {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        } else {
            reqId.set(expectAuditRequestId(threadContext));
        }
        assertThat(result, notNullValue());
        assertThat(expectAuditRequestId(threadContext), is(result.v2()));
        assertThat(result.v1().getUser(), sameInstance(anonymousUser));
        assertThat(result.v1().getAuthenticationType(), is(AuthenticationType.ANONYMOUS));
        assertThreadContextContainsAuthentication(result.v1());
        verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result.v1()), eq(threadContext));
    }

    public void testAnonymousUserTransportWithDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(settings, realms, auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool, anonymousUser, tokenService, apiKeyService, serviceAccountService, operatorPrivilegesService);

        InternalRequest message = new InternalRequest();
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }

        Tuple<Authentication, String> result = authenticateBlocking("_action", message, SystemUser.INSTANCE);
        if (requestIdAlreadyPresent) {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        } else {
            reqId.set(expectAuditRequestId(threadContext));
        }
        assertThat(result, notNullValue());
        assertThat(expectAuditRequestId(threadContext), is(result.v2()));
        assertThat(result.v1().getUser(), sameInstance(SystemUser.INSTANCE));
        assertThat(result.v1().getAuthenticationType(), is(AuthenticationType.INTERNAL));
        assertThreadContextContainsAuthentication(result.v1());
        verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result.v1()), eq(threadContext));
    }

    public void testRealmTokenThrowingException() throws Exception {
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        when(firstRealm.token(threadContext)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            authenticateBlocking("_action", transportRequest, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).authenticationFailed(reqId.get(), "_action", transportRequest);
            verifyZeroInteractions(operatorPrivilegesService);
        }
    }

    public void testRealmTokenThrowingExceptionRest() throws Exception {
        when(firstRealm.token(threadContext)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            authenticateBlocking(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            String reqId = expectAuditRequestId(threadContext);
            verify(auditTrail).authenticationFailed(reqId, restRequest);
            verifyZeroInteractions(operatorPrivilegesService);
        }
    }

    public void testRealmSupportsMethodThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenThrow(authenticationError("realm doesn't like supports"));
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        try {
            authenticateBlocking("_action", transportRequest, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like supports"));
            verify(auditTrail).authenticationFailed(reqId, token, "_action", transportRequest);
            verifyZeroInteractions(operatorPrivilegesService);
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
            String reqId = expectAuditRequestId(threadContext);
            verify(auditTrail).authenticationFailed(reqId, token, restRequest);
            verifyZeroInteractions(operatorPrivilegesService);
        }
    }

    public void testRealmAuthenticateTerminateAuthenticationProcessWithException() {
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        final AuthenticationToken token = mock(AuthenticationToken.class);
        final String principal = randomAlphaOfLength(5);
        when(token.principal()).thenReturn(principal);
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        final boolean throwElasticsearchSecurityException = randomBoolean();
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
        mockAuthenticate(secondRealm, token, throwE, true);

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", transportRequest, null));
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
        if (requestIdAlreadyPresent) {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        } else {
            reqId.set(expectAuditRequestId(threadContext));
        }
        verify(auditTrail).authenticationFailed(reqId.get(), secondRealm.name(), token, "_action", transportRequest);
        verify(auditTrail).authenticationFailed(reqId.get(), token, "_action", transportRequest);
        verifyNoMoreInteractions(auditTrail);
        verifyZeroInteractions(operatorPrivilegesService);
    }

    public void testRealmAuthenticateGracefulTerminateAuthenticationProcess() {
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        final AuthenticationToken token = mock(AuthenticationToken.class);
        final String principal = randomAlphaOfLength(5);
        when(token.principal()).thenReturn(principal);
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        final String basicScheme = "Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\"";
        mockAuthenticate(firstRealm, token, null, true);

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", transportRequest, null));
            assertThat(e.getMessage(), is("unable to authenticate user [" + principal + "] for action [_action]"));
            assertThat(e.getHeader("WWW-Authenticate"), contains(basicScheme));
        if (requestIdAlreadyPresent) {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        } else {
            reqId.set(expectAuditRequestId(threadContext));
        }
        verify(auditTrail).authenticationFailed(reqId.get(), firstRealm.name(), token, "_action", transportRequest);
        verify(auditTrail).authenticationFailed(reqId.get(), token, "_action", transportRequest);
        verifyNoMoreInteractions(auditTrail);
        verifyZeroInteractions(operatorPrivilegesService);
    }

    public void testRealmAuthenticateThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        doThrow(authenticationError("realm doesn't like authenticate"))
            .when(secondRealm).authenticate(eq(token), anyActionListener());
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        try {
            authenticateBlocking("_action", transportRequest, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like authenticate"));
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).authenticationFailed(reqId.get(), token, "_action", transportRequest);
            verifyZeroInteractions(operatorPrivilegesService);
        }
    }

    public void testRealmAuthenticateThrowingExceptionRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        doThrow(authenticationError("realm doesn't like authenticate"))
                .when(secondRealm).authenticate(eq(token), anyActionListener());
        try {
            authenticateBlocking(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), is("realm doesn't like authenticate"));
            String reqId = expectAuditRequestId(threadContext);
            verify(auditTrail).authenticationFailed(reqId, token, restRequest);
            verifyZeroInteractions(operatorPrivilegesService);
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
            .when(secondRealm).lookupUser(eq("run_as"), anyActionListener());
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        try {
            authenticateBlocking("_action", transportRequest, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).authenticationFailed(reqId.get(), token, "_action", transportRequest);
            verifyZeroInteractions(operatorPrivilegesService);
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
                .when(secondRealm).lookupUser(eq("run_as"), anyActionListener());
        try {
            authenticateBlocking(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            String reqId = expectAuditRequestId(threadContext);
            verify(auditTrail).authenticationFailed(reqId, token, restRequest);
            verifyZeroInteractions(operatorPrivilegesService);
        }
    }

    public void testRunAsLookupSameRealm() throws Exception {
        boolean testTransportRequest = randomBoolean();
        boolean requestIdAlreadyPresent = randomBoolean() && testTransportRequest;
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
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
            @SuppressWarnings("unchecked")
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), anyActionListener());

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
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                expectAuditRequestId(threadContext);
            }
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
            setCompletedToTrue(completed);
        }, this::logAndFail);

        // we do not actually go async
        if (testTransportRequest) {
            service.authenticate("_action", transportRequest, true, listener);
        } else {
            service.authenticate(restRequest, listener);
        }
        assertTrue(completed.get());
    }

    @SuppressWarnings("unchecked")
    public void testRunAsLookupDifferentRealm() throws Exception {
        boolean testTransportRequest = randomBoolean();
        boolean requestIdAlreadyPresent = randomBoolean() && testTransportRequest;
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
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
        }).when(firstRealm).lookupUser(eq("run_as"), anyActionListener());

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
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                expectAuditRequestId(threadContext);
            }
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
            setCompletedToTrue(completed);
        }, this::logAndFail);

        // call service asynchronously but it doesn't actually go async
        if (testTransportRequest) {
            service.authenticate("_action", transportRequest, true, listener);
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
            String reqId = expectAuditRequestId(threadContext);
            verify(auditTrail).runAsDenied(eq(reqId), any(Authentication.class), eq(restRequest), eq(EmptyAuthorizationInfo.INSTANCE));
            verifyNoMoreInteractions(auditTrail);
            verifyZeroInteractions(operatorPrivilegesService);
        }
    }

    public void testRunAsWithEmptyRunAsUsername() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        User user = new User("lookup user", new String[]{"user"});
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "");
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);

        try {
            authenticateBlocking("_action", transportRequest, null);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).runAsDenied(eq(reqId.get()), any(Authentication.class), eq("_action"), eq(transportRequest),
                eq(EmptyAuthorizationInfo.INSTANCE));
            verifyNoMoreInteractions(auditTrail);
            verifyZeroInteractions(operatorPrivilegesService);
        }
    }

    @SuppressWarnings("unchecked")
    public void testAuthenticateTransportDisabledRunAsUser() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doAnswer((i) -> {
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}, null, null, Map.of(), false));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), anyActionListener());
        User fallback = randomBoolean() ? SystemUser.INSTANCE : null;
        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", transportRequest, fallback));
        if (requestIdAlreadyPresent) {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        } else {
            reqId.set(expectAuditRequestId(threadContext));
        }
        verify(auditTrail).authenticationFailed(reqId.get(), token, "_action", transportRequest);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
        verifyZeroInteractions(operatorPrivilegesService);
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
        }).when(secondRealm).lookupUser(eq("run_as"), anyActionListener());

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking(restRequest));
        String reqId = expectAuditRequestId(threadContext);
        verify(auditTrail).authenticationFailed(reqId, token, restRequest);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
        verifyZeroInteractions(operatorPrivilegesService);
    }

    public void testAuthenticateWithToken() throws Exception {
        User user = new User("_username", "r1");
        final AtomicBoolean completed = new AtomicBoolean(false);
        final Authentication expected = new Authentication(user, new RealmRef("realm", "custom", "node"), null);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        final String userTokenId = UUIDs.randomBase64UUID();
        final String refreshToken = UUIDs.randomBase64UUID();
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            Authentication originatingAuth = new Authentication(new User("creator"), new RealmRef("test", "test", "test"), null);
            tokenService.createOAuth2Tokens(userTokenId, refreshToken, expected, originatingAuth, Collections.emptyMap(), tokenFuture);
        }
        String token = tokenFuture.get().getAccessToken();
        when(client.prepareMultiGet()).thenReturn(new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE));
        mockGetTokenFromId(tokenService, userTokenId, expected, false, client);
        when(securityIndex.freeze()).thenReturn(securityIndex);
        when(securityIndex.isAvailable()).thenReturn(true);
        when(securityIndex.indexExists()).thenReturn(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", "Bearer " + token);
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            service.authenticate("_action", transportRequest, true, ActionListener.wrap(result -> {
                    assertThat(result, notNullValue());
                    assertThat(result.getUser(), is(user));
                    assertThat(result.getLookedUpBy(), is(nullValue()));
                    assertThat(result.getAuthenticatedBy(), is(notNullValue()));
                    assertThat(result.getAuthenticatedBy().getName(), is("realm")); // TODO implement equals
                    assertThat(result.getAuthenticationType(), is(AuthenticationType.TOKEN));
                    if (requestIdAlreadyPresent) {
                        assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
                    } else {
                        reqId.set(expectAuditRequestId(threadContext));
                    }
                    verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
                    setCompletedToTrue(completed);
                    verify(auditTrail).authenticationSuccess(eq(reqId.get()), eq(result), eq("_action"), same(transportRequest));
                }, this::logAndFail));
        }
        assertTrue(completed.get());
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
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", "Bearer " + Base64.getEncoder().encodeToString(randomBytes));
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            service.authenticate("_action", transportRequest, true, ActionListener.wrap(result -> {
                assertThat(result, notNullValue());
                assertThat(result.getUser(), is(user));
                assertThat(result.getLookedUpBy(), is(nullValue()));
                assertThat(result.getAuthenticatedBy(), is(notNullValue()));
                assertThreadContextContainsAuthentication(result);
                assertEquals(expected, result);
                if (requestIdAlreadyPresent) {
                    assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
                } else {
                    reqId.set(expectAuditRequestId(threadContext));
                }
                verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
                success.set(true);
                latch.countDown();
            }, e -> {
                verifyZeroInteractions(operatorPrivilegesService);
                if (requestIdAlreadyPresent) {
                    assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
                } else {
                    reqId.set(expectAuditRequestId(threadContext));
                }
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
            verify(auditTrail).authenticationSuccess(eq(reqId.get()), eq(expected), eq("_action"), same(transportRequest));
        }
        verifyNoMoreInteractions(auditTrail);
    }

    public void testExpiredToken() throws Exception {
        when(securityIndex.freeze()).thenReturn(securityIndex);
        when(securityIndex.isAvailable()).thenReturn(true);
        when(securityIndex.indexExists()).thenReturn(true);
        User user = new User("_username", "r1");
        final Authentication expected = new Authentication(user, new RealmRef("realm", "custom", "node"), null);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        final String userTokenId = UUIDs.randomBase64UUID();
        final String refreshToken = UUIDs.randomBase64UUID();
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            Authentication originatingAuth = new Authentication(new User("creator"), new RealmRef("test", "test", "test"), null);
            tokenService.createOAuth2Tokens(userTokenId, refreshToken, expected, originatingAuth, Collections.emptyMap(), tokenFuture);
        }
        String token = tokenFuture.get().getAccessToken();
        mockGetTokenFromId(tokenService, userTokenId, expected, true, client);
        doAnswer(invocationOnMock -> {
            ((Runnable) invocationOnMock.getArguments()[1]).run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(anyConsumer(), any(Runnable.class));

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            threadContext.putHeader("Authorization", "Bearer " + token);
            ElasticsearchSecurityException e =
                    expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", transportRequest, null));
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            }
            assertEquals(RestStatus.UNAUTHORIZED, e.status());
            assertEquals("token expired", e.getMessage());
            verifyZeroInteractions(operatorPrivilegesService);
        }
    }

    public void testApiKeyAuthInvalidHeader() {
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            final String invalidHeader = randomFrom("apikey", "apikey ", "apikey foo");
            threadContext.putHeader("Authorization", invalidHeader);
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> authenticateBlocking("_action", transportRequest, null));
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            }
            assertEquals(RestStatus.UNAUTHORIZED, e.status());
            assertThat(e.getMessage(), containsString("missing authentication credentials"));
            verifyZeroInteractions(operatorPrivilegesService);
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
                source.put("expiration_time", null);
                source.put("api_key_invalidated", false);
                source.put("api_key_hash", new String(Hasher.BCRYPT4.hash(new SecureString(key.toCharArray()))));
                source.put("role_descriptors", Collections.singletonMap("api key role", Collections.singletonMap("cluster", "all")));
                source.put("limited_by_role_descriptors",
                    Collections.singletonMap("limited api key role", Collections.singletonMap("cluster", "all")));
                source.put("name", "my api key for testApiKeyAuth");
                source.put("version", 0);
                Map<String, Object> creatorMap = new HashMap<>();
                creatorMap.put("principal", "johndoe");
                creatorMap.put("full_name", "john doe");
                creatorMap.put("email", "john@doe.com");
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
        }).when(client).get(any(GetRequest.class), anyActionListener());

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            threadContext.putHeader("Authorization", headerValue);
            Tuple <Authentication, String> result = authenticateBlocking("_action", transportRequest, null);
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            }
            assertThat(expectAuditRequestId(threadContext), is(result.v2()));
            assertThat(result.v1().getUser().principal(), is("johndoe"));
            assertThat(result.v1().getUser().fullName(), is("john doe"));
            assertThat(result.v1().getUser().email(), is("john@doe.com"));
            assertThat(result.v1().getAuthenticationType(), is(AuthenticationType.API_KEY));
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result.v1()), eq(threadContext));
        }
    }

    public void testExpiredApiKey() {
        final String id = randomAlphaOfLength(12);
        final String key = UUIDs.randomBase64UUID(random());
        final String headerValue = "ApiKey " + Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
        doAnswer(invocationOnMock -> {
            final GetRequest request = (GetRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
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
        }).when(client).get(any(GetRequest.class), anyActionListener());

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            threadContext.putHeader("Authorization", headerValue);
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> authenticateBlocking("_action", transportRequest, null));
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            }
            assertEquals(RestStatus.UNAUTHORIZED, e.status());
            verifyZeroInteractions(operatorPrivilegesService);
        }
    }

    public void testCanAuthenticateServiceAccount() throws ExecutionException, InterruptedException {
        Mockito.reset(serviceAccountService);
        final TokenInfo.TokenSource tokenSource = randomFrom(TokenInfo.TokenSource.values());
        final Authentication authentication = new Authentication(
            new User("elastic/fleet-server"),
            new RealmRef("_service_account", "_service_account", "foo"), null,
            Version.CURRENT, AuthenticationType.TOKEN,
            Map.of("_token_name", ValidationTests.randomTokenName(), "_token_source", tokenSource.name().toLowerCase(Locale.ROOT)));
        try (ThreadContext.StoredContext ignored = threadContext.newStoredContext(false)) {
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            threadContext.putHeader("Authorization", "Bearer AAEAAWVsYXN0aWMvZmxlZXQtc2VydmVyL3Rva2VuMTpyNXdkYmRib1FTZTl2R09Ld2FKR0F3");
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<Authentication> listener = (ActionListener<Authentication>) invocationOnMock.getArguments()[2];
                listener.onResponse(authentication);
                return null;
            }).when(serviceAccountService).authenticateToken(any(), any(), any());
            final Tuple<Authentication, String> result = authenticateBlocking("_action", transportRequest, null);
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            }
            assertThat(expectAuditRequestId(threadContext), is(result.v2()));
            assertThat(result.v1(), is(authentication));
            verify(auditTrail).authenticationSuccess(eq(result.v2()), same(authentication), eq("_action"), same(transportRequest));
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result.v1()), eq(threadContext));
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testServiceAccountFailureWillNotFallthrough() throws IOException {
        Mockito.reset(serviceAccountService);
        final ElasticsearchSecurityException bailOut = new ElasticsearchSecurityException("bail out", RestStatus.UNAUTHORIZED);
        try (ThreadContext.StoredContext ignored = threadContext.newStoredContext(false)) {
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            final String bearerString = "AAEAAWVsYXN0aWMvZmxlZXQtc2VydmVyL3Rva2VuMTpyNXdkYmRib1FTZTl2R09Ld2FKR0F3";
            threadContext.putHeader("Authorization", "Bearer " + bearerString);
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<Authentication> listener = (ActionListener<Authentication>) invocationOnMock.getArguments()[2];
                listener.onFailure(bailOut);
                return null;
            }).when(serviceAccountService).authenticateToken(any(), any(), any());
            final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> authenticateBlocking("_action", transportRequest, null));
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            assertThat(e, sameInstance(bailOut));
            verifyZeroInteractions(operatorPrivilegesService);
            final ServiceAccountToken serviceToken = ServiceAccountToken.fromBearerString(new SecureString(bearerString.toCharArray()));
            verify(auditTrail).authenticationFailed(eq(reqId.get()), eq(serviceToken), eq("_action"), eq(transportRequest));
        }
    }

    private static class InternalRequest extends TransportRequest {
        @Override
        public void writeTo(StreamOutput out) {}
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
        }).when(realm).authenticate(eq(token), anyActionListener());
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
        }).when(realm).authenticate(eq(token), anyActionListener());
    }

    private Tuple<Authentication, String> authenticateBlocking(RestRequest restRequest) {
        SetOnce<String> reqId = new SetOnce<>();
        PlainActionFuture<Authentication> future = new PlainActionFuture<>() {
            @Override
            public void onResponse(Authentication result) {
                reqId.set(expectAuditRequestId(threadContext));
                assertThat(new AuthenticationContextSerializer().getAuthentication(threadContext), is(result));
                super.onResponse(result);
            }

            @Override
            public void onFailure(Exception e) {
                reqId.set(expectAuditRequestId(threadContext));
                super.onFailure(e);
            }
        };
        service.authenticate(restRequest, future);
        Authentication authentication = future.actionGet();
        assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        return new Tuple<>(authentication, reqId.get());
    }

    private Tuple<Authentication, String> authenticateBlocking(String action, TransportRequest transportRequest, User fallbackUser) {
        SetOnce<String> reqId = new SetOnce<>();
        PlainActionFuture<Authentication> future = new PlainActionFuture<>() {
            @Override
            public void onResponse(Authentication result) {
                reqId.set(expectAuditRequestId(threadContext));
                assertThat(new AuthenticationContextSerializer().getAuthentication(threadContext), is(result));
                super.onResponse(result);
            }

            @Override
            public void onFailure(Exception e) {
                reqId.set(expectAuditRequestId(threadContext));
                super.onFailure(e);
            }
        };
        if (fallbackUser == null) {
            service.authenticate(action, transportRequest, true, future);
        } else {
            service.authenticate(action, transportRequest, fallbackUser, future);
        }
        Authentication authentication = future.actionGet();
        assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        return new Tuple<>(authentication, reqId.get());
    }

    private static String expectAuditRequestId(ThreadContext threadContext) {
        String reqId = AuditUtil.extractRequestId(threadContext);
        assertThat(reqId, is(not(emptyOrNullString())));
        return reqId;
    }

    @SuppressWarnings("unchecked")
    private static void mockRealmLookupReturnsNull(Realm realm, String username) {
        doAnswer((i) -> {
            ActionListener<?> listener = (ActionListener<?>) i.getArguments()[1];
            listener.onResponse(null);
            return null;
        }).when(realm).lookupUser(eq(username), anyActionListener());
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
            Instant.now(), true, true, true, null, concreteSecurityIndexName, indexStatus, IndexMetadata.State.OPEN, null, "my_uuid");
    }

    @SuppressWarnings("unchecked")
    private static <T> Consumer<T> anyConsumer() {
        return any(Consumer.class);
    }
}
