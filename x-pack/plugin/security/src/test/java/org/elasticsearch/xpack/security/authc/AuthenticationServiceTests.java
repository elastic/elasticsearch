/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.Level;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.Realm.Factory;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.EmptyAuthorizationInfo;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;
import static org.elasticsearch.xpack.security.Security.SECURITY_CRYPTO_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.security.authc.TokenService.THREAD_POOL_NAME;
import static org.elasticsearch.xpack.security.authc.TokenServiceTests.mockGetTokenFromAccessTokenBytes;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
    private RealmDomain firstDomain;
    private Realm firstRealm;
    private RealmDomain secondDomain;
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
    private SecurityIndexManager.IndexState projectIndex;
    private Client client;
    private InetSocketAddress remoteAddress;
    private OperatorPrivileges.OperatorPrivilegesService operatorPrivilegesService;
    private String concreteSecurityIndexName;

    @Before
    @SuppressForbidden(reason = "Allow accessing localhost")
    public void init() throws Exception {
        concreteSecurityIndexName = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );

        token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        transportRequest = new InternalRequest();
        remoteAddress = new InetSocketAddress(InetAddress.getLocalHost(), 100);
        transportRequest.remoteAddress(remoteAddress);
        restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withRemoteAddress(remoteAddress).build();
        threadContext = new ThreadContext(Settings.EMPTY);

        firstDomain = randomFrom(new RealmDomain("firstDomain", Set.of()), null);
        firstRealm = mock(Realm.class);
        when(firstRealm.type()).thenReturn(FIRST_REALM_TYPE);
        when(firstRealm.name()).thenReturn(FIRST_REALM_NAME);
        when(firstRealm.toString()).thenReturn(FIRST_REALM_NAME + "/" + FIRST_REALM_TYPE);
        when(firstRealm.realmRef()).thenReturn(new RealmRef(FIRST_REALM_NAME, FIRST_REALM_TYPE, "authc_test", firstDomain));
        secondDomain = randomFrom(new RealmDomain("secondDomain", Set.of()), null);
        secondRealm = mock(Realm.class);
        when(secondRealm.type()).thenReturn(SECOND_REALM_TYPE);
        when(secondRealm.name()).thenReturn(SECOND_REALM_NAME);
        when(secondRealm.toString()).thenReturn(SECOND_REALM_NAME + "/" + SECOND_REALM_TYPE);
        when(secondRealm.realmRef()).thenReturn(new RealmRef(SECOND_REALM_NAME, SECOND_REALM_TYPE, "authc_test", secondDomain));
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("node.name", "authc_test")
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true)
            .build();
        MockLicenseState licenseState = mock(MockLicenseState.class);
        for (String realmType : InternalRealms.getConfigurableRealmsTypes()) {
            final LicensedFeature.Persistent feature = InternalRealms.getLicensedFeature(realmType);
            if (feature != null) {
                when(licenseState.isAllowed(feature)).thenReturn(true);
            }
        }
        when(licenseState.isAllowed(Security.CUSTOM_REALMS_FEATURE)).thenReturn(true);
        when(licenseState.isAllowed(Security.TOKEN_SERVICE_FEATURE)).thenReturn(true);
        when(licenseState.copyCurrentLicenseState()).thenReturn(licenseState);
        when(licenseState.isAllowed(Security.AUDITING_FEATURE)).thenReturn(true);
        when(licenseState.getOperationMode()).thenReturn(randomFrom(License.OperationMode.ENTERPRISE, License.OperationMode.PLATINUM));

        ReservedRealm reservedRealm = mock(ReservedRealm.class);
        when(reservedRealm.type()).thenReturn("reserved");
        when(reservedRealm.name()).thenReturn("reserved_realm");
        realms = spy(
            new TestRealms(
                Settings.EMPTY,
                TestEnvironment.newEnvironment(settings),
                Map.of(FileRealmSettings.TYPE, this::mockRealm, NativeRealmSettings.TYPE, this::mockRealm),
                licenseState,
                threadContext,
                reservedRealm,
                Arrays.asList(firstRealm, secondRealm),
                Arrays.asList(firstRealm)
            )
        );

        // Needed because this is calculated in the constructor, which means the override doesn't get called correctly
        realms.recomputeActiveRealms();
        assertThat(realms.getActiveRealms(), contains(firstRealm, secondRealm));

        auditTrail = mock(AuditTrail.class);
        auditTrailService = new AuditTrailService(auditTrail, licenseState);
        client = mock(Client.class);
        threadPool = new ThreadPool(
            settings,
            MeterRegistry.NOOP,
            new DefaultBuiltInExecutorBuilders(),
            new FixedExecutorBuilder(
                settings,
                THREAD_POOL_NAME,
                1,
                1000,
                "xpack.security.authc.token.thread_pool",
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            ),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                SECURITY_CRYPTO_THREAD_POOL_NAME,
                1,
                1000,
                "xpack.security.crypto.thread_pool",
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            )
        );
        threadContext = threadPool.getThreadContext();
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        when(client.prepareIndex(nullable(String.class))).thenReturn(new IndexRequestBuilder(client));
        when(client.prepareUpdate(nullable(String.class), nullable(String.class))).thenReturn(new UpdateRequestBuilder(client));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndexResponse> responseActionListener = (ActionListener<IndexResponse>) invocationOnMock.getArguments()[2];
            responseActionListener.onResponse(
                new IndexResponse(
                    new ShardId(".security", UUIDs.randomBase64UUID(), randomInt()),
                    randomAlphaOfLength(4),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    true
                )
            );
            return null;
        }).when(client).execute(eq(TransportIndexAction.TYPE), any(IndexRequest.class), anyActionListener());
        doAnswer(invocationOnMock -> {
            GetRequestBuilder builder = new GetRequestBuilder(client);
            builder.setIndex((String) invocationOnMock.getArguments()[0]).setId((String) invocationOnMock.getArguments()[1]);
            return builder;
        }).when(client).prepareGet(nullable(String.class), nullable(String.class));
        securityIndex = mock(SecurityIndexManager.class);
        projectIndex = mock(SecurityIndexManager.IndexState.class);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(projectIndex).prepareIndexIfNeededThenExecute(anyConsumer(), any(Runnable.class));
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(projectIndex).checkIndexVersionThenExecute(anyConsumer(), any(Runnable.class));
        final ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Sets.union(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                Set.of(ApiKeyService.DELETE_RETENTION_PERIOD, ApiKeyService.DELETE_INTERVAL)
            )
        );
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings);
        final SecurityContext securityContext = new SecurityContext(settings, threadContext);
        apiKeyService = new ApiKeyService(
            settings,
            Clock.systemUTC(),
            client,
            securityIndex,
            clusterService,
            mock(CacheInvalidatorRegistry.class),
            threadPool,
            MeterRegistry.NOOP
        );
        tokenService = new TokenService(
            settings,
            Clock.systemUTC(),
            client,
            licenseState,
            securityContext,
            securityIndex,
            securityIndex,
            clusterService
        );
        serviceAccountService = mock(ServiceAccountService.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Authentication> listener = (ActionListener<Authentication>) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return null;
        }).when(serviceAccountService).authenticateToken(any(), any(), any());

        operatorPrivilegesService = mock(OperatorPrivileges.OperatorPrivilegesService.class);
        service = new AuthenticationService(
            settings,
            realms,
            auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool,
            new AnonymousUser(settings),
            tokenService,
            apiKeyService,
            serviceAccountService,
            operatorPrivilegesService,
            mock(),
            MeterRegistry.NOOP
        );
    }

    private Realm mockRealm(RealmConfig config) {
        Class<? extends Realm> cls = switch (config.type()) {
            case InternalRealms.FILE_TYPE -> FileRealm.class;
            case InternalRealms.NATIVE_TYPE -> NativeRealm.class;
            default -> throw new IllegalArgumentException("No factory for realm " + config);
        };
        final Realm mock = mock(cls);
        when(mock.type()).thenReturn(config.type());
        when(mock.name()).thenReturn(config.name());
        when(mock.order()).thenReturn(config.order());
        return mock;
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
        when(firstRealm.supports(token)).thenReturn(false);
        when(secondRealm.supports(token)).thenReturn(true);
        final User user = new User(randomAlphaOfLength(8));
        final AuthenticationResult<User> authenticationResult = AuthenticationResult.success(user);
        doAnswer(invocationOnMock -> {
            final Object[] arguments = invocationOnMock.getArguments();
            assertThat(arguments[0], is(token));
            @SuppressWarnings("unchecked")
            final ActionListener<AuthenticationResult<User>> listener = (ActionListener<AuthenticationResult<User>>) arguments[1];
            listener.onResponse(authenticationResult);
            return null;
        }).when(secondRealm).authenticate(eq(token), anyActionListener());

        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate("action", transportRequest, true, ActionListener.wrap(authentication -> {
            assertThat(threadContext.getTransient(AuthenticationResult.THREAD_CONTEXT_KEY), is(authenticationResult));
            assertThat(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY), is(authentication));
            assertThat(authentication.getEffectiveSubject().getRealm().getDomain(), is(secondDomain));
            verify(auditTrail).authenticationSuccess(anyString(), eq(authentication), eq("action"), eq(transportRequest));
            setCompletedToTrue(completed);
        }, this::logAndFail));
        assertThat(completed.get(), is(true));
        verifyNoMoreInteractions(auditTrail);
    }

    public void testTokenMissing() throws Exception {
        try (var mockLog = MockLog.capture(RealmsAuthenticator.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "unlicensed realms",
                    RealmsAuthenticator.class.getName(),
                    Level.WARN,
                    "No authentication credential could be extracted using realms [file_realm/file]. "
                        + "Realms [second_realm/second] were skipped because they are not permitted on the current license"
                )
            );

            Mockito.doReturn(List.of(secondRealm)).when(realms).getUnlicensedRealms();
            Mockito.doReturn(List.of(firstRealm)).when(realms).getActiveRealms();
            SetOnce<String> reqId = new SetOnce<>();

            final boolean requestIdAlreadyPresent = randomBoolean();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            // requestId is always generated for rest request and should not already have it
            final boolean isRestRequest = randomBoolean() && false == requestIdAlreadyPresent;

            final AtomicBoolean completed = new AtomicBoolean(false);
            final ActionListener<Authentication> listener = ActionListener.wrap(authentication -> { fail("should not reach here"); }, e -> {
                if (requestIdAlreadyPresent) {
                    assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
                } else {
                    reqId.set(expectAuditRequestId(threadContext));
                }
                assertThat(e, isA(ElasticsearchSecurityException.class));
                assertThat(e.getMessage(), containsString("missing authentication credentials"));
                if (isRestRequest) {
                    verify(auditTrail).anonymousAccessDenied(reqId.get(), restRequest.getHttpRequest());
                } else {
                    verify(auditTrail).anonymousAccessDenied(reqId.get(), "_action", transportRequest);
                }
                verifyNoMoreInteractions(auditTrail);
                mockLog.assertAllExpectationsMatched();
                setCompletedToTrue(completed);
            });

            if (isRestRequest) {
                service.authenticate(restRequest.getHttpRequest(), true, listener);
            } else {
                service.authenticate("_action", transportRequest, true, listener);
            }
            assertThat(completed.get(), is(true));
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
            assertThat(result.getEffectiveSubject().getUser(), is(user));
            assertThat(result.isRunAs(), is(false));
            assertThat(result.getAuthenticatingSubject().getRealm(), is(notNullValue())); // TODO implement equals
            assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
        }, this::logAndFail));
        assertTrue(completed.get());
        verify(auditTrail).authenticationFailed(reqId.get(), firstRealm.name(), token, "_action", transportRequest);
        verify(realms, atLeastOnce()).recomputeActiveRealms();
        verify(realms, atLeastOnce()).calculateLicensedRealms(any(XPackLicenseState.class));
        verify(realms, atLeastOnce()).getActiveRealms();
        verify(realms, atLeastOnce()).handleDisabledRealmDueToLicenseChange(any(Realm.class), any(XPackLicenseState.class));
        // ^^ We don't care how many times these methods are called, we just check it here so that we can verify no more interactions below.
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
            assertThat(result.getEffectiveSubject().getUser(), is(user));
            assertThat(result.isRunAs(), is(false));
            assertThat(result.getAuthenticatingSubject().getRealm(), is(notNullValue())); // TODO implement equals
            assertThat(result.getAuthenticatingSubject().getRealm().getName(), is(SECOND_REALM_NAME));
            assertThat(result.getAuthenticatingSubject().getRealm().getType(), is(SECOND_REALM_TYPE));
            assertThat(result.getAuthenticatingSubject().getRealm().getDomain(), is(secondDomain));
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
        reset(operatorPrivilegesService);
        service.authenticate("_action", transportRequest, true, ActionListener.wrap(result -> {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            assertThat(result, notNullValue());
            assertThat(result.getEffectiveSubject().getUser(), is(user));
            assertThat(result.isRunAs(), is(false));
            assertThat(result.getAuthenticatingSubject().getRealm(), is(notNullValue())); // TODO implement equals
            assertThat(result.getAuthenticatingSubject().getRealm().getName(), is(SECOND_REALM_NAME));
            assertThat(result.getAuthenticatingSubject().getRealm().getType(), is(SECOND_REALM_TYPE));
            assertThat(result.getAuthenticatingSubject().getRealm().getDomain(), is(secondDomain));
            assertThreadContextContainsAuthentication(result);
            verify(auditTrail, times(2)).authenticationSuccess(reqId.get(), result, "_action", transportRequest);
            setCompletedToTrue(completed);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
        }, this::logAndFail));

        verify(auditTrail).authenticationFailed(reqId.get(), firstRealm.name(), token, "_action", transportRequest);
        verify(firstRealm, times(4)).name(); // used above one time plus two times for authc result and time metrics
        verify(firstRealm, times(2)).type(); // used two times to collect authc result and time metrics
        verify(secondRealm, times(2)).realmRef(); // also used in license tracking
        verify(firstRealm, times(2)).token(threadContext);
        verify(secondRealm, times(2)).token(threadContext);
        verify(firstRealm).supports(token);
        verify(secondRealm, times(2)).supports(token);
        verify(firstRealm).authenticate(eq(token), anyActionListener());
        verify(secondRealm, times(2)).authenticate(eq(token), anyActionListener());
        verify(secondRealm, times(4)).name(); // called two times for every authenticate call to collect authc result and time metrics
        verify(secondRealm, times(4)).type(); // called two times for every authenticate call to collect authc result and time metrics
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
            assertThat(result.getEffectiveSubject().getUser(), is(user));
            assertThat(result.isRunAs(), is(false));
            assertThat(result.getAuthenticatingSubject().getRealm(), is(notNullValue()));
            assertThat(result.getAuthenticatingSubject().getRealm().getName(), is(FIRST_REALM_NAME));
            assertThat(result.getAuthenticatingSubject().getRealm().getType(), is(FIRST_REALM_TYPE));
            assertThat(result.getAuthenticatingSubject().getRealm().getDomain(), is(firstDomain));
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
        SecurityIndexManager.IndexState previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        SecurityIndexManager.IndexState currentState = dummyState(null);
        service.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(++expectedInvalidation, service.getNumInvalidation());

        // doesn't exist to exists
        previousState = dummyState(null);
        currentState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        service.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(++expectedInvalidation, service.getNumInvalidation());

        // green or yellow to red
        previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentState = dummyState(ClusterHealthStatus.RED);
        service.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(expectedInvalidation, service.getNumInvalidation());

        // red to non red
        previousState = dummyState(ClusterHealthStatus.RED);
        currentState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        service.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(++expectedInvalidation, service.getNumInvalidation());

        // green to yellow or yellow to green
        previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentState = dummyState(
            previousState.indexHealth == ClusterHealthStatus.GREEN ? ClusterHealthStatus.YELLOW : ClusterHealthStatus.GREEN
        );
        service.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(expectedInvalidation, service.getNumInvalidation());
    }

    public void testAuthenticateSmartRealmOrderingDisabled() {
        final Settings settings = Settings.builder().put(AuthenticationService.SUCCESS_AUTH_CACHE_ENABLED.getKey(), false).build();
        service = new AuthenticationService(
            settings,
            realms,
            auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool,
            new AnonymousUser(Settings.EMPTY),
            tokenService,
            apiKeyService,
            serviceAccountService,
            operatorPrivilegesService,
            mock(),
            MeterRegistry.NOOP
        );
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
            assertThat(result.getEffectiveSubject().getUser(), is(user));
            assertThat(result.isRunAs(), is(false));
            assertThat(result.getAuthenticatingSubject().getRealm().getName(), is(SECOND_REALM_NAME)); // TODO implement equals
            assertThat(result.getAuthenticatingSubject().getRealm().getDomain(), is(secondDomain));
            assertThreadContextContainsAuthentication(result);
            verify(auditTrail).authenticationSuccess(reqId.get(), result, "_action", transportRequest);
            setCompletedToTrue(completed);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
        }, this::logAndFail));
        assertTrue(completed.get());

        completed.set(false);
        reset(operatorPrivilegesService);
        service.authenticate("_action", transportRequest, true, ActionListener.wrap(result -> {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            assertThat(result, notNullValue());
            assertThat(result.getEffectiveSubject().getUser(), is(user));
            assertThat(result.isRunAs(), is(false));
            assertThat(result.getAuthenticatingSubject().getRealm().getName(), is(SECOND_REALM_NAME)); // TODO implement equals
            assertThat(result.getAuthenticatingSubject().getRealm().getDomain(), is(secondDomain));
            assertThreadContextContainsAuthentication(result);
            verify(auditTrail, times(2)).authenticationSuccess(reqId.get(), result, "_action", transportRequest);
            setCompletedToTrue(completed);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result), eq(threadContext));
        }, this::logAndFail));
        verify(auditTrail, times(2)).authenticationFailed(reqId.get(), firstRealm.name(), token, "_action", transportRequest);
        verify(firstRealm, times(7)).name(); // used above one time plus two times for every call to collect success and time metrics
        verify(firstRealm, times(4)).type(); // used two times for every call to collect authc result and time metrics
        verify(secondRealm, times(2)).realmRef();
        verify(firstRealm, times(2)).token(threadContext);
        verify(secondRealm, times(2)).token(threadContext);
        verify(firstRealm, times(2)).supports(token);
        verify(secondRealm, times(2)).supports(token);
        verify(firstRealm, times(2)).authenticate(eq(token), anyActionListener());
        verify(secondRealm, times(2)).authenticate(eq(token), anyActionListener());
        verify(secondRealm, times(4)).name(); // called two times for every authenticate call to collect authc result and time metrics
        verify(secondRealm, times(4)).type(); // called two times for every authenticate call to collect authc result and time metrics
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
            assertThat(result.getEffectiveSubject().getUser(), is(user));
            assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
            assertThat(result.getAuthenticatingSubject().getRealm().getName(), is(secondRealm.name())); // TODO implement equals
            assertThat(result.getAuthenticatingSubject().getRealm().getDomain(), is(secondDomain));
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
        final Authentication authentication = AuthenticationTestHelper.builder()
            .realm()
            .user(new User("_username", "r1"))
            .realmRef(new RealmRef("test", "cached", "foo", randomFrom(new RealmDomain("", Set.of()), null)))
            .build(false);
        authentication.writeToContext(threadContext);
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }

        authenticateBlocking("_action", transportRequest, null, result -> {
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            }
            assertThat(expectAuditRequestId(threadContext), is(result.v2()));
            assertThat(result, notNullValue());
            assertThat(result.v1(), is(authentication));
            assertThat(result.v1().getAuthenticationType(), is(AuthenticationType.REALM));
            verifyNoMoreInteractions(auditTrail);
            verifyNoMoreInteractions(firstRealm, secondRealm);
            verify(operatorPrivilegesService, times(1)).maybeMarkOperatorUser(result.v1(), threadContext);
        });
    }

    public void testAuthenticateNonExistentRestRequestUserThrowsAuthenticationException() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(
            new UsernamePasswordToken("idonotexist", new SecureString("passwd".toCharArray()))
        );
        try {
            authenticateBlocking(restRequest, null);
            fail("Authentication was successful but should not");
        } catch (ElasticsearchSecurityException e) {
            expectAuditRequestId(threadContext);
            assertAuthenticationException(e, containsString("unable to authenticate user [idonotexist] for REST request [/]"));
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
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
            assertThat(result.getEffectiveSubject().getUser(), is(user));
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
            authenticateBlocking("_action", transportRequest, null, null);
            fail("expected an authentication exception when trying to authenticate an anonymous message");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthenticationException(e);
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
        if (requestIdAlreadyPresent) {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        } else {
            reqId.set(expectAuditRequestId(threadContext));
        }
        verify(auditTrail).anonymousAccessDenied(reqId.get(), "_action", transportRequest);
    }

    public void testAuthenticateRestAnonymous() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);
        try {
            authenticateBlocking(restRequest, null);
            fail("expected an authentication exception when trying to authenticate an anonymous message");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthenticationException(e);
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
        verify(auditTrail).anonymousAccessDenied(expectAuditRequestId(threadContext), restRequest.getHttpRequest());
    }

    public void testAuthenticateTransportFallback() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);
        User fallbackUser = AuthenticationTestHelper.randomInternalUser();
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }

        authenticateBlocking("_action", transportRequest, fallbackUser, result -> {
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            }
            assertThat(expectAuditRequestId(threadContext), is(result.v2()));
            assertThat(result, notNullValue());
            assertThat(result.v1().getEffectiveSubject().getUser(), sameInstance(fallbackUser));
            assertThat(result.v1().getAuthenticationType(), is(AuthenticationType.INTERNAL));
            assertThreadContextContainsAuthentication(result.v1());
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result.v1()), eq(threadContext));
        });
    }

    public void testAuthenticateTransportDisabledUser() throws Exception {
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        User user = new User("username", new String[] { "r1", "r2" }, null, null, Map.of(), false);
        User fallback = randomBoolean() ? InternalUsers.SYSTEM_USER : null;
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authenticateBlocking("_action", transportRequest, fallback, null)
        );
        if (requestIdAlreadyPresent) {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        } else {
            reqId.set(expectAuditRequestId(threadContext));
        }
        verify(auditTrail).authenticationFailed(reqId.get(), token, "_action", transportRequest);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
        verifyNoMoreInteractions(operatorPrivilegesService);
    }

    public void testAuthenticateRestDisabledUser() throws Exception {
        User user = new User("username", new String[] { "r1", "r2" }, null, null, Map.of(), false);
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authenticateBlocking(restRequest, null)
        );
        String reqId = expectAuditRequestId(threadContext);
        verify(auditTrail).authenticationFailed(reqId, token, restRequest.getHttpRequest());
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
        verifyNoMoreInteractions(operatorPrivilegesService);
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
            authenticate = listener -> service.authenticate("_action", transportRequest, InternalUsers.SYSTEM_USER, listener);
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
            assertThat(result.getEffectiveSubject().getUser(), sameInstance(user));
            assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
            assertThat(result.getAuthenticatingSubject().getRealm().getDomain(), is(firstDomain));
            assertThat(result.getAuthenticatingSubject().getRealm().getName(), is(firstRealm.name())); // TODO implement equals
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
        service.authenticate(restRequest.getHttpRequest(), ActionListener.wrap(authentication -> {
            auditTrailService.get().authenticationSuccess(restRequest);
            assertThat(authentication, notNullValue());
            assertThat(authentication.getEffectiveSubject().getUser(), sameInstance(user1));
            assertThat(authentication.getAuthenticationType(), is(AuthenticationType.REALM));
            assertThat(authentication.getAuthenticatingSubject().getRealm().getName(), is(firstRealm.name())); // TODO implement equals
            assertThat(authentication.getAuthenticatingSubject().getRealm().getDomain(), is(firstDomain)); // TODO implement equals
            assertThreadContextContainsAuthentication(authentication);
            String reqId = expectAuditRequestId(threadContext);
            verify(auditTrail).authenticationSuccess(restRequest);
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
            service.authenticate("_action", transportRequest, InternalUsers.SYSTEM_USER, ActionListener.wrap(authentication -> {
                if (requestIdAlreadyPresent) {
                    assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
                } else {
                    reqId.set(expectAuditRequestId(threadContext));
                }
                assertThat(authentication, notNullValue());
                assertThat(authentication.getEffectiveSubject().getUser(), sameInstance(user1));
                assertThat(authentication.getAuthenticationType(), is(AuthenticationType.REALM));
                assertThat(authentication.getEffectiveSubject().getRealm().getDomain(), is(firstDomain));
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
        reset(operatorPrivilegesService);
        try {
            ThreadContext threadContext1 = threadPool1.getThreadContext();
            service = new AuthenticationService(
                Settings.EMPTY,
                realms,
                auditTrailService,
                new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
                threadPool1,
                new AnonymousUser(Settings.EMPTY),
                tokenService,
                apiKeyService,
                serviceAccountService,
                operatorPrivilegesService,
                mock(),
                MeterRegistry.NOOP
            );
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext1));
            }

            threadContext1.putTransient(AuthenticationField.AUTHENTICATION_KEY, authRef.get());
            threadContext1.putHeader(AuthenticationField.AUTHENTICATION_KEY, authHeaderRef.get());
            service.authenticate("_action", message1, InternalUsers.SYSTEM_USER, ActionListener.wrap(ctxAuth -> {
                if (requestIdAlreadyPresent) {
                    assertThat(expectAuditRequestId(threadContext1), is(reqId.get()));
                } else {
                    reqId.set(expectAuditRequestId(threadContext1));
                }
                assertThat(ctxAuth, sameInstance(authRef.get()));
                assertThat(threadContext1.getHeader(AuthenticationField.AUTHENTICATION_KEY), sameInstance(authHeaderRef.get()));
                setCompletedToTrue(completed);
                verify(operatorPrivilegesService, times(1)).maybeMarkOperatorUser(ctxAuth, threadContext1);
            }, this::logAndFail));
            assertTrue(completed.compareAndSet(true, false));
            verifyNoMoreInteractions(firstRealm);
            reset(firstRealm);
        } finally {
            terminate(threadPool1);
        }

        // checking authentication from the user header
        ThreadPool threadPool2 = new TestThreadPool("testAutheticateTransportContextAndHeader2");
        reset(operatorPrivilegesService);
        try {
            ThreadContext threadContext2 = threadPool2.getThreadContext();
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext2));
            }
            final String header;
            try (ThreadContext.StoredContext ignore = threadContext2.stashContext()) {
                service = new AuthenticationService(
                    Settings.EMPTY,
                    realms,
                    auditTrailService,
                    new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
                    threadPool2,
                    new AnonymousUser(Settings.EMPTY),
                    tokenService,
                    apiKeyService,
                    serviceAccountService,
                    operatorPrivilegesService,
                    mock(),
                    MeterRegistry.NOOP
                );
                threadContext2.putHeader(AuthenticationField.AUTHENTICATION_KEY, authHeaderRef.get());

                BytesStreamOutput output = new BytesStreamOutput();
                threadContext2.writeTo(output);
                StreamInput input = output.bytes().streamInput();
                threadContext2 = new ThreadContext(Settings.EMPTY);
                threadContext2.readHeaders(input);
                header = threadContext2.getHeader(AuthenticationField.AUTHENTICATION_KEY);
            }

            threadPool2.getThreadContext().putHeader(AuthenticationField.AUTHENTICATION_KEY, header);
            service = new AuthenticationService(
                Settings.EMPTY,
                realms,
                auditTrailService,
                new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
                threadPool2,
                new AnonymousUser(Settings.EMPTY),
                tokenService,
                apiKeyService,
                serviceAccountService,
                operatorPrivilegesService,
                mock(),
                MeterRegistry.NOOP
            );
            service.authenticate("_action", new InternalRequest(), InternalUsers.SYSTEM_USER, ActionListener.wrap(result -> {
                if (requestIdAlreadyPresent) {
                    assertThat(expectAuditRequestId(threadPool2.getThreadContext()), is(reqId.get()));
                } else {
                    reqId.set(expectAuditRequestId(threadPool2.getThreadContext()));
                }
                assertThat(result, notNullValue());
                assertThat(result.getEffectiveSubject().getUser(), equalTo(user1));
                assertThat(result.getAuthenticationType(), is(AuthenticationType.REALM));
                setCompletedToTrue(completed);
                verify(operatorPrivilegesService, times(1)).maybeMarkOperatorUser(result, threadPool2.getThreadContext());
            }, this::logAndFail));
            assertTrue(completed.get());
            verifyNoMoreInteractions(firstRealm);
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
            authenticateBlocking("_action", message, randomBoolean() ? InternalUsers.SYSTEM_USER : null, null);
        } catch (Exception e) {
            // expected
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).tamperedRequest(reqId.get(), "_action", message);
            verifyNoMoreInteractions(auditTrail);
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
    }

    public void testWrongTokenDoesNotFallbackToAnonymous() {
        String username = randomBoolean() ? AnonymousUser.DEFAULT_ANONYMOUS_USERNAME : "user1";
        Settings.Builder builder = Settings.builder().putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3");
        if (username.equals(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME) == false) {
            builder.put(AnonymousUser.USERNAME_SETTING.getKey(), username);
        }
        Settings anonymousEnabledSettings = builder.build();
        final AnonymousUser anonymousUser = new AnonymousUser(anonymousEnabledSettings);
        service = new AuthenticationService(
            anonymousEnabledSettings,
            realms,
            auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool,
            anonymousUser,
            tokenService,
            apiKeyService,
            serviceAccountService,
            operatorPrivilegesService,
            mock(),
            MeterRegistry.NOOP
        );

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            threadContext.putHeader("Authorization", "Bearer thisisaninvalidtoken");
            ElasticsearchSecurityException e = expectThrows(
                ElasticsearchSecurityException.class,
                () -> authenticateBlocking("_action", transportRequest, null, null)
            );
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).anonymousAccessDenied(reqId.get(), "_action", transportRequest);
            verifyNoMoreInteractions(auditTrail);
            assertAuthenticationException(e);
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
    }

    public void testWrongApiKeyDoesNotFallbackToAnonymous() {
        String username = randomBoolean() ? AnonymousUser.DEFAULT_ANONYMOUS_USERNAME : "user1";
        Settings.Builder builder = Settings.builder().putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3");
        if (username.equals(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME) == false) {
            builder.put(AnonymousUser.USERNAME_SETTING.getKey(), username);
        }
        Settings anonymousEnabledSettings = builder.build();
        final AnonymousUser anonymousUser = new AnonymousUser(anonymousEnabledSettings);
        service = new AuthenticationService(
            anonymousEnabledSettings,
            realms,
            auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool,
            anonymousUser,
            tokenService,
            apiKeyService,
            serviceAccountService,
            operatorPrivilegesService,
            mock(),
            MeterRegistry.NOOP
        );
        doAnswer(invocationOnMock -> {
            final GetRequest request = (GetRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            final ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(
                new GetResponse(
                    new GetResult(
                        request.index(),
                        request.id(),
                        UNASSIGNED_SEQ_NO,
                        UNASSIGNED_PRIMARY_TERM,
                        -1L,
                        false,
                        null,
                        Collections.emptyMap(),
                        Collections.emptyMap()
                    )
                )
            );
            return Void.TYPE;
        }).when(client).get(any(GetRequest.class), anyActionListener());
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            threadContext.putHeader("Authorization", "ApiKey dGhpc2lzYW5pbnZhbGlkaWQ6dGhpc2lzYW5pbnZhbGlkc2VjcmV0");
            ElasticsearchSecurityException e = expectThrows(
                ElasticsearchSecurityException.class,
                () -> authenticateBlocking("_action", transportRequest, null, null)
            );
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).anonymousAccessDenied(reqId.get(), "_action", transportRequest);
            verifyNoMoreInteractions(auditTrail);
            assertAuthenticationException(e);
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
    }

    public void testAnonymousUserRest() throws Exception {
        String username = randomBoolean() ? AnonymousUser.DEFAULT_ANONYMOUS_USERNAME : "user1";
        Settings.Builder builder = Settings.builder().putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3");
        if (username.equals(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME) == false) {
            builder.put(AnonymousUser.USERNAME_SETTING.getKey(), username);
        }
        Settings settings = builder.build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(
            settings,
            realms,
            auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool,
            anonymousUser,
            tokenService,
            apiKeyService,
            serviceAccountService,
            operatorPrivilegesService,
            mock(),
            MeterRegistry.NOOP
        );
        RestRequest request = new FakeRestRequest();

        authenticateBlocking(request, result -> {
            assertThat(result, notNullValue());
            assertThat(result.v1().getEffectiveSubject().getUser(), sameInstance((Object) anonymousUser));
            assertThat(result.v1().getAuthenticationType(), is(AuthenticationType.ANONYMOUS));
            assertThat(result.v1().getEffectiveSubject().getRealm().getDomain(), nullValue());
            assertThreadContextContainsAuthentication(result.v1());
            assertThat(expectAuditRequestId(threadContext), is(result.v2()));
            verify(auditTrail).authenticationSuccess(request);
            verifyNoMoreInteractions(auditTrail);
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result.v1()), eq(threadContext));
        });
    }

    public void testAuthenticateRestRequestDisallowAnonymous() throws Exception {
        final String username = randomBoolean() ? AnonymousUser.DEFAULT_ANONYMOUS_USERNAME : "_anon_" + randomAlphaOfLengthBetween(2, 6);
        final Settings.Builder builder = Settings.builder().putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3");
        if (username.equals(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME) == false) {
            builder.put(AnonymousUser.USERNAME_SETTING.getKey(), username);
        }
        Settings settings = builder.build();

        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(
            settings,
            realms,
            auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool,
            anonymousUser,
            tokenService,
            apiKeyService,
            serviceAccountService,
            operatorPrivilegesService,
            mock(),
            MeterRegistry.NOOP
        );
        RestRequest request = new FakeRestRequest();

        PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticate(request.getHttpRequest(), false, future);
        final ElasticsearchSecurityException ex = expectThrows(ElasticsearchSecurityException.class, future::actionGet);

        assertThat(ex, notNullValue());
        assertThat(ex, throwableWithMessage(containsString("missing authentication credentials for REST request")));
        assertThat(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY), nullValue());
        assertThat(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY), nullValue());
        String reqId = expectAuditRequestId(threadContext);
        verify(auditTrail).anonymousAccessDenied(reqId, request.getHttpRequest());
        verifyNoMoreInteractions(auditTrail);
        verifyNoMoreInteractions(operatorPrivilegesService);
    }

    public void testAnonymousUserTransportNoDefaultUser() throws Exception {
        Settings settings = Settings.builder().putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(
            settings,
            realms,
            auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool,
            anonymousUser,
            tokenService,
            apiKeyService,
            serviceAccountService,
            operatorPrivilegesService,
            mock(),
            MeterRegistry.NOOP
        );
        InternalRequest message = new InternalRequest();
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }

        authenticateBlocking("_action", message, null, result -> {
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            }
            assertThat(expectAuditRequestId(threadContext), is(result.v2()));
            assertThat(result.v1().getEffectiveSubject().getUser(), sameInstance(anonymousUser));
            assertThat(result.v1().getAuthenticationType(), is(AuthenticationType.ANONYMOUS));
            assertThat(result.v1().getEffectiveSubject().getRealm().getDomain(), nullValue());
            assertThreadContextContainsAuthentication(result.v1());
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result.v1()), eq(threadContext));
        });
    }

    public void testAnonymousUserTransportWithDefaultUser() throws Exception {
        Settings settings = Settings.builder().putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(
            settings,
            realms,
            auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool,
            anonymousUser,
            tokenService,
            apiKeyService,
            serviceAccountService,
            operatorPrivilegesService,
            mock(),
            MeterRegistry.NOOP
        );

        InternalRequest message = new InternalRequest();
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }

        authenticateBlocking("_action", message, InternalUsers.SYSTEM_USER, result -> {
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            }
            assertThat(result, notNullValue());
            assertThat(expectAuditRequestId(threadContext), is(result.v2()));
            assertThat(result.v1().getEffectiveSubject().getUser(), sameInstance(InternalUsers.SYSTEM_USER));
            assertThat(result.v1().getAuthenticationType(), is(AuthenticationType.INTERNAL));
            assertThat(result.v1().getEffectiveSubject().getRealm().getDomain(), nullValue());
            assertThreadContextContainsAuthentication(result.v1());
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result.v1()), eq(threadContext));
        });
    }

    public void testRealmTokenThrowingException() throws Exception {
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        when(firstRealm.token(threadContext)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            authenticateBlocking("_action", transportRequest, null, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).authenticationFailed(reqId.get(), "_action", transportRequest);
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
    }

    public void testRealmTokenThrowingExceptionRest() throws Exception {
        when(firstRealm.token(threadContext)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            authenticateBlocking(restRequest, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            String reqId = expectAuditRequestId(threadContext);
            verify(auditTrail).authenticationFailed(reqId, restRequest.getHttpRequest());
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
    }

    public void testRealmSupportsMethodThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenThrow(authenticationError("realm doesn't like supports"));
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        try {
            authenticateBlocking("_action", transportRequest, null, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like supports"));
            verify(auditTrail).authenticationFailed(reqId, token, "_action", transportRequest);
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
    }

    public void testRealmSupportsMethodThrowingExceptionRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenThrow(authenticationError("realm doesn't like supports"));
        try {
            authenticateBlocking(restRequest, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like supports"));
            String reqId = expectAuditRequestId(threadContext);
            verify(auditTrail).authenticationFailed(reqId, token, restRequest.getHttpRequest());
            verifyNoMoreInteractions(operatorPrivilegesService);
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
        final String basicScheme = "Basic realm=\"" + XPackField.SECURITY + "\", charset=\"UTF-8\"";
        String selectedScheme = randomFrom(basicScheme, "Negotiate IOJoj");
        if (throwElasticsearchSecurityException) {
            throwE = new ElasticsearchSecurityException("authentication error", RestStatus.UNAUTHORIZED);
            if (withAuthenticateHeader) {
                ((ElasticsearchSecurityException) throwE).addHeader("WWW-Authenticate", selectedScheme);
            }
        }
        mockAuthenticate(secondRealm, token, throwE, true);

        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authenticateBlocking("_action", transportRequest, null, null)
        );
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
        verifyNoMoreInteractions(operatorPrivilegesService);
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
        final String basicScheme = "Basic realm=\"" + XPackField.SECURITY + "\", charset=\"UTF-8\"";
        mockAuthenticate(firstRealm, token, null, true);

        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authenticateBlocking("_action", transportRequest, null, null)
        );
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
        verifyNoMoreInteractions(operatorPrivilegesService);
    }

    public void testRealmAuthenticateThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        doThrow(authenticationError("realm doesn't like authenticate")).when(secondRealm).authenticate(eq(token), anyActionListener());
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        try {
            authenticateBlocking("_action", transportRequest, null, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like authenticate"));
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).authenticationFailed(reqId.get(), token, "_action", transportRequest);
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
    }

    public void testRealmAuthenticateThrowingExceptionRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        doThrow(authenticationError("realm doesn't like authenticate")).when(secondRealm).authenticate(eq(token), anyActionListener());
        try {
            authenticateBlocking(restRequest, null);
            fail("exception should bubble out");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), is("realm doesn't like authenticate"));
            String reqId = expectAuditRequestId(threadContext);
            verify(auditTrail).authenticationFailed(reqId, token, restRequest.getHttpRequest());
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
    }

    public void testRealmLookupThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[] { "user" }));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doThrow(authenticationError("realm doesn't want to lookup")).when(secondRealm).lookupUser(eq("run_as"), anyActionListener());
        boolean requestIdAlreadyPresent = randomBoolean();
        SetOnce<String> reqId = new SetOnce<>();
        if (requestIdAlreadyPresent) {
            reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
        }
        try {
            authenticateBlocking("_action", transportRequest, null, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).authenticationFailed(reqId.get(), token, "_action", transportRequest);
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
    }

    public void testRealmLookupThrowingExceptionRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[] { "user" }));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doThrow(authenticationError("realm doesn't want to lookup")).when(secondRealm).lookupUser(eq("run_as"), anyActionListener());
        try {
            authenticateBlocking(restRequest, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            String reqId = expectAuditRequestId(threadContext);
            verify(auditTrail).authenticationFailed(reqId, token, restRequest.getHttpRequest());
            verifyNoMoreInteractions(operatorPrivilegesService);
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
        final User user = new User("lookup user", new String[] { "user" }, "lookup user", "lookup@foo.foo", Map.of("foo", "bar"), true);
        mockAuthenticate(secondRealm, token, user);
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doAnswer((i) -> {
            @SuppressWarnings("unchecked")
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[] { "some role" }));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), anyActionListener());

        final AtomicBoolean completed = new AtomicBoolean(false);
        ActionListener<Authentication> listener = ActionListener.wrap(authentication -> {
            assertThat(authentication, notNullValue());
            assertThat(authentication.getAuthenticationType(), is(AuthenticationType.REALM));
            User effectiveUser = authentication.getEffectiveSubject().getUser();

            assertThat(effectiveUser.principal(), is("looked up user"));
            assertThat(effectiveUser.roles(), arrayContaining("some role"));
            assertThreadContextContainsAuthentication(authentication);

            assertThat(SystemUser.is(effectiveUser), is(false));
            assertThat(authentication.isRunAs(), is(true));
            User authUser = authentication.getAuthenticatingSubject().getUser();
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
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(authentication), eq(threadContext));
            setCompletedToTrue(completed);
        }, this::logAndFail);

        // we do not actually go async
        if (testTransportRequest) {
            service.authenticate("_action", transportRequest, true, listener);
        } else {
            service.authenticate(restRequest.getHttpRequest(), listener);
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
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[] { "user" }));
        doAnswer((i) -> {
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[] { "some role" }));
            return null;
        }).when(firstRealm).lookupUser(eq("run_as"), anyActionListener());

        final AtomicBoolean completed = new AtomicBoolean(false);
        ActionListener<Authentication> listener = ActionListener.wrap(authentication -> {
            assertThat(authentication, notNullValue());
            assertThat(authentication.getAuthenticationType(), is(AuthenticationType.REALM));
            final User effectiveUser = authentication.getEffectiveSubject().getUser();
            final User authenticatingUser = authentication.getAuthenticatingSubject().getUser();

            assertThat(SystemUser.is(effectiveUser), is(false));
            assertThat(authentication.isRunAs(), is(true));
            assertThat(authenticatingUser.principal(), is("lookup user"));
            assertThat(authenticatingUser.roles(), arrayContaining("user"));
            assertThat(effectiveUser.principal(), is("looked up user"));
            assertThat(effectiveUser.roles(), arrayContaining("some role"));
            assertThreadContextContainsAuthentication(authentication);
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                expectAuditRequestId(threadContext);
            }
            verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(authentication), eq(threadContext));
            setCompletedToTrue(completed);
        }, this::logAndFail);

        // call service asynchronously but it doesn't actually go async
        if (testTransportRequest) {
            service.authenticate("_action", transportRequest, true, listener);
        } else {
            service.authenticate(restRequest.getHttpRequest(), listener);
        }
        assertTrue(completed.get());
    }

    public void testRunAsWithEmptyRunAsUsernameRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        User user = new User("lookup user", new String[] { "user" });
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);

        try {
            authenticateBlocking(restRequest, null);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            String reqId = expectAuditRequestId(threadContext);
            verify(auditTrail).runAsDenied(
                eq(reqId),
                any(Authentication.class),
                eq(restRequest.getHttpRequest()),
                eq(EmptyAuthorizationInfo.INSTANCE)
            );
            verifyNoMoreInteractions(auditTrail);
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
    }

    public void testRunAsWithEmptyRunAsUsername() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        User user = new User("lookup user", new String[] { "user" });
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
            authenticateBlocking("_action", transportRequest, null, null);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            verify(auditTrail).runAsDenied(
                eq(reqId.get()),
                any(Authentication.class),
                eq("_action"),
                eq(transportRequest),
                eq(EmptyAuthorizationInfo.INSTANCE)
            );
            verifyNoMoreInteractions(auditTrail);
            verifyNoMoreInteractions(operatorPrivilegesService);
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
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[] { "user" }));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doAnswer((i) -> {
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[] { "some role" }, null, null, Map.of(), false));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), anyActionListener());
        User fallback = randomBoolean() ? InternalUsers.SYSTEM_USER : null;
        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authenticateBlocking("_action", transportRequest, fallback, null)
        );
        if (requestIdAlreadyPresent) {
            assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
        } else {
            reqId.set(expectAuditRequestId(threadContext));
        }
        verify(auditTrail).authenticationFailed(reqId.get(), token, "_action", transportRequest);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
        verifyNoMoreInteractions(operatorPrivilegesService);
    }

    public void testAuthenticateRestDisabledRunAsUser() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(token.principal()).thenReturn(randomAlphaOfLength(5));
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[] { "user" }));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doAnswer((i) -> {
            @SuppressWarnings("unchecked")
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[] { "some role" }, null, null, Map.of(), false));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), anyActionListener());

        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authenticateBlocking(restRequest, null)
        );
        String reqId = expectAuditRequestId(threadContext);
        verify(auditTrail).authenticationFailed(reqId, token, restRequest.getHttpRequest());
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
        verifyNoMoreInteractions(operatorPrivilegesService);
    }

    public void testAuthenticateWithToken() throws Exception {
        User user = new User("_username", "r1");
        final AtomicBoolean completed = new AtomicBoolean(false);
        final Authentication expected = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new RealmRef("realm", "custom", "node"))
            .build(false);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(randomBoolean());
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            Authentication originatingAuth = AuthenticationTestHelper.builder()
                .user(new User("creator"))
                .realmRef(new RealmRef("test", "test", "test"))
                .build(false);
            tokenService.createOAuth2Tokens(
                newTokenBytes.v1(),
                newTokenBytes.v2(),
                expected,
                originatingAuth,
                Collections.emptyMap(),
                tokenFuture
            );
        }
        String token = tokenFuture.get().getAccessToken();
        when(client.prepareMultiGet()).thenReturn(new MultiGetRequestBuilder(client));
        mockGetTokenFromAccessTokenBytes(tokenService, newTokenBytes.v1(), expected, Map.of(), false, null, client);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(true);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        when(projectIndex.indexExists()).thenReturn(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", "Bearer " + token);
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            service.authenticate("_action", transportRequest, true, ActionListener.wrap(result -> {
                assertThat(result, notNullValue());
                assertThat(result.getEffectiveSubject().getUser(), is(user));
                assertThat(result.isRunAs(), is(false));
                assertThat(result.getAuthenticatingSubject().getRealm(), is(notNullValue()));
                assertThat(result.getAuthenticatingSubject().getRealm().getName(), is("realm")); // TODO implement equals
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

        SecurityIndexManager.IndexState projectIndex = mock(SecurityIndexManager.IndexState.class);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);
        // An invalid token might decode to something that looks like a UUID
        // Randomise it being invalid because the index doesn't exist, or the document doesn't exist
        if (randomBoolean()) {
            when(projectIndex.isAvailable(any())).thenReturn(false);
            when(projectIndex.getUnavailableReason(any())).thenReturn(new ElasticsearchException(getTestName()));
        } else {
            when(projectIndex.isAvailable(any())).thenReturn(true);
            doAnswer(inv -> {
                final GetRequest request = inv.getArgument(0);
                final ActionListener<GetResponse> listener = inv.getArgument(1);
                listener.onResponse(
                    new GetResponse(
                        new GetResult(
                            request.index(),
                            request.id(),
                            UNASSIGNED_SEQ_NO,
                            UNASSIGNED_PRIMARY_TERM,
                            0,
                            false,
                            null,
                            Map.of(),
                            Map.of()
                        )
                    )
                );
                return null;
            }).when(client).get(any(GetRequest.class), any());
        }

        mockAuthenticate(firstRealm, token, user);
        final int numBytes = randomIntBetween(TokenService.MINIMUM_BYTES, TokenService.MINIMUM_BYTES + 32);
        final byte[] randomBytes = new byte[numBytes];
        random().nextBytes(randomBytes);
        final CountDownLatch latch = new CountDownLatch(1);
        final Authentication expected = AuthenticationTestHelper.builder()
            .realm()
            .user(user)
            .realmRef(new RealmRef(firstRealm.name(), firstRealm.type(), "authc_test", firstDomain))
            .build(false);
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
                assertThat(result.getEffectiveSubject().getUser(), is(user));
                assertThat(result.isRunAs(), is(false));
                assertThat(result.getAuthenticatingSubject().getRealm(), is(notNullValue()));
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
                verifyNoMoreInteractions(operatorPrivilegesService);
                if (requestIdAlreadyPresent) {
                    assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
                } else {
                    reqId.set(expectAuditRequestId(threadContext));
                }
                if (e instanceof IllegalStateException) {
                    assertThat(e.getMessage(), containsString("array length must be <= to " + ArrayUtil.MAX_ARRAY_LENGTH + " but was: "));
                    latch.countDown();
                } else if (e instanceof NegativeArraySizeException) {
                    assertThat(e.getMessage(), containsString("array size must be positive but was: "));
                    latch.countDown();
                } else if (e instanceof ElasticsearchException) {
                    assertThat(e.getMessage(), containsString(getTestName()));
                    latch.countDown();
                } else {
                    logger.error("unexpected exception", e);
                    latch.countDown();
                    fail("unexpected exception: " + e.getMessage());
                }
            }));
        } catch (IllegalStateException ex) {
            assertThat(ex.getMessage(), containsString("array length must be <= to " + ArrayUtil.MAX_ARRAY_LENGTH + " but was: "));
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
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(true);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        when(projectIndex.indexExists()).thenReturn(true);
        User user = new User("_username", "r1");
        final Authentication expected = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new RealmRef("realm", "custom", "node"))
            .build(false);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(randomBoolean());
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            Authentication originatingAuth = AuthenticationTestHelper.builder()
                .user(new User("creator"))
                .realmRef(new RealmRef("test", "test", "test"))
                .build(false);
            tokenService.createOAuth2Tokens(
                newTokenBytes.v1(),
                newTokenBytes.v2(),
                expected,
                originatingAuth,
                Collections.emptyMap(),
                tokenFuture
            );
        }
        String token = tokenFuture.get().getAccessToken();
        mockGetTokenFromAccessTokenBytes(tokenService, newTokenBytes.v1(), expected, Map.of(), true, null, client);

        doAnswer(invocationOnMock -> {
            ((Runnable) invocationOnMock.getArguments()[1]).run();
            return null;
        }).when(projectIndex).prepareIndexIfNeededThenExecute(anyConsumer(), any(Runnable.class));

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            boolean requestIdAlreadyPresent = randomBoolean();
            SetOnce<String> reqId = new SetOnce<>();
            if (requestIdAlreadyPresent) {
                reqId.set(AuditUtil.getOrGenerateRequestId(threadContext));
            }
            threadContext.putHeader("Authorization", "Bearer " + token);
            ElasticsearchSecurityException e = expectThrows(
                ElasticsearchSecurityException.class,
                () -> authenticateBlocking("_action", transportRequest, null, null)
            );
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            }
            assertEquals(RestStatus.UNAUTHORIZED, e.status());
            assertEquals("token expired", e.getMessage());
            verifyNoMoreInteractions(operatorPrivilegesService);
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
            ElasticsearchSecurityException e = expectThrows(
                ElasticsearchSecurityException.class,
                () -> authenticateBlocking("_action", transportRequest, null, null)
            );
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            }
            assertEquals(RestStatus.UNAUTHORIZED, e.status());
            if (invalidHeader.equals("apikey foo")) {
                assertThat(
                    e.getMessage(),
                    containsString("unable to authenticate with provided credentials and anonymous access is not allowed for this request")
                );
            } else {
                assertThat(e.getMessage(), containsString("missing authentication credentials"));
            }
            verifyNoMoreInteractions(operatorPrivilegesService);
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
                source.put(
                    "limited_by_role_descriptors",
                    Collections.singletonMap("limited api key role", Collections.singletonMap("cluster", "all"))
                );
                source.put("name", "my api key for testApiKeyAuth");
                source.put("version", 0);
                Map<String, Object> creatorMap = new HashMap<>();
                creatorMap.put("principal", "johndoe");
                creatorMap.put("full_name", "john doe");
                creatorMap.put("email", "john@doe.com");
                creatorMap.put("metadata", Collections.emptyMap());
                creatorMap.put("realm", "auth realm");
                source.put("creator", creatorMap);
                GetResponse getResponse = new GetResponse(
                    new GetResult(
                        request.index(),
                        request.id(),
                        0,
                        1,
                        1L,
                        true,
                        BytesReference.bytes(JsonXContent.contentBuilder().map(source)),
                        Collections.emptyMap(),
                        Collections.emptyMap()
                    )
                );
                listener.onResponse(getResponse);
            } else {
                listener.onResponse(
                    new GetResponse(
                        new GetResult(
                            request.index(),
                            request.id(),
                            UNASSIGNED_SEQ_NO,
                            1,
                            -1L,
                            false,
                            null,
                            Collections.emptyMap(),
                            Collections.emptyMap()
                        )
                    )
                );
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
            authenticateBlocking("_action", transportRequest, null, result -> {
                if (requestIdAlreadyPresent) {
                    assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
                }
                assertThat(expectAuditRequestId(threadContext), is(result.v2()));
                assertThat(result.v1().getEffectiveSubject().getUser().principal(), is("johndoe"));
                assertThat(result.v1().getEffectiveSubject().getUser().fullName(), is("john doe"));
                assertThat(result.v1().getEffectiveSubject().getUser().email(), is("john@doe.com"));
                assertThat(result.v1().getAuthenticationType(), is(AuthenticationType.API_KEY));
                assertThat(result.v1().getEffectiveSubject().getRealm().getDomain(), nullValue());
                verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result.v1()), eq(threadContext));
            });
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
                GetResponse getResponse = new GetResponse(
                    new GetResult(
                        request.index(),
                        request.id(),
                        0,
                        1,
                        1L,
                        true,
                        BytesReference.bytes(JsonXContent.contentBuilder().map(source)),
                        Collections.emptyMap(),
                        Collections.emptyMap()
                    )
                );
                listener.onResponse(getResponse);
            } else {
                listener.onResponse(
                    new GetResponse(
                        new GetResult(
                            request.index(),
                            request.id(),
                            UNASSIGNED_SEQ_NO,
                            1,
                            -1L,
                            false,
                            null,
                            Collections.emptyMap(),
                            Collections.emptyMap()
                        )
                    )
                );
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
            ElasticsearchSecurityException e = expectThrows(
                ElasticsearchSecurityException.class,
                () -> authenticateBlocking("_action", transportRequest, null, null)
            );
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            }
            assertEquals(RestStatus.UNAUTHORIZED, e.status());
            verifyNoMoreInteractions(operatorPrivilegesService);
        }
    }

    public void testCanAuthenticateServiceAccount() {
        reset(serviceAccountService);
        final Authentication authentication = AuthenticationTestHelper.builder().serviceAccount().build();
        try (ThreadContext.StoredContext ignored = threadContext.newStoredContext()) {
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
            authenticateBlocking("_action", transportRequest, null, result -> {
                if (requestIdAlreadyPresent) {
                    assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
                }
                assertThat(expectAuditRequestId(threadContext), is(result.v2()));
                assertThat(result.v1(), is(authentication));
                verify(auditTrail).authenticationSuccess(eq(result.v2()), same(authentication), eq("_action"), same(transportRequest));
                verify(operatorPrivilegesService).maybeMarkOperatorUser(eq(result.v1()), eq(threadContext));
                verifyNoMoreInteractions(auditTrail);
            });
        }
    }

    public void testServiceAccountFailureWillNotFallthrough() throws IOException {
        reset(serviceAccountService);
        final ElasticsearchSecurityException bailOut = new ElasticsearchSecurityException("bail out", RestStatus.UNAUTHORIZED);
        try (ThreadContext.StoredContext ignored = threadContext.newStoredContext()) {
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
            final ElasticsearchSecurityException e = expectThrows(
                ElasticsearchSecurityException.class,
                () -> authenticateBlocking("_action", transportRequest, null, null)
            );
            if (requestIdAlreadyPresent) {
                assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
            } else {
                reqId.set(expectAuditRequestId(threadContext));
            }
            assertThat(e, sameInstance(bailOut));
            verifyNoMoreInteractions(operatorPrivilegesService);
            final ServiceAccountToken serviceToken = ServiceAccountToken.fromBearerString(new SecureString(bearerString.toCharArray()));
            verify(auditTrail).authenticationFailed(
                eq(reqId.get()),
                argThat(o -> ((ServiceAccountToken) o).getTokenId().equals(serviceToken.getTokenId())),
                eq("_action"),
                eq(transportRequest)
            );
        }
    }

    static class InternalRequest extends AbstractTransportRequest {
        @Override
        public void writeTo(StreamOutput out) {}
    }

    void assertThreadContextContainsAuthentication(Authentication authentication) {
        Authentication contextAuth = threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY);
        assertThat(contextAuth, notNullValue());
        assertThat(contextAuth, is(authentication));
        try {
            assertThat(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY), equalTo((Object) authentication.encode()));
        } catch (IOException e) {
            fail("should not error");
        }
    }

    @SuppressWarnings("unchecked")
    private void mockAuthenticate(Realm realm, AuthenticationToken token, User user) {
        final boolean separateThread = randomBoolean();
        doAnswer(i -> {
            ActionListener<AuthenticationResult<User>> listener = (ActionListener<AuthenticationResult<User>>) i.getArguments()[1];
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
            ActionListener<AuthenticationResult<User>> listener = (ActionListener<AuthenticationResult<User>>) i.getArguments()[1];
            if (terminate) {
                listener.onResponse(AuthenticationResult.terminate("terminate authc process", e));
            } else {
                listener.onResponse(AuthenticationResult.unsuccessful("unsuccessful, but continue authc process", e));
            }
            return null;
        }).when(realm).authenticate(eq(token), anyActionListener());
    }

    private void authenticateBlocking(RestRequest restRequest, Consumer<Tuple<Authentication, String>> verifier) {
        SetOnce<String> reqId = new SetOnce<>();
        PlainActionFuture<Authentication> future = new PlainActionFuture<>() {
            @Override
            public void onResponse(Authentication result) {
                auditTrailService.get().authenticationSuccess(restRequest);
                reqId.set(expectAuditRequestId(threadContext));
                assertThat(new AuthenticationContextSerializer().getAuthentication(threadContext), is(result));
                if (verifier != null) {
                    verifier.accept(new Tuple<>(result, reqId.get()));
                }
                super.onResponse(result);
            }

            @Override
            public void onFailure(Exception e) {
                reqId.set(expectAuditRequestId(threadContext));
                super.onFailure(e);
            }
        };
        service.authenticate(restRequest.getHttpRequest(), future);
        future.actionGet();
        assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
    }

    private void authenticateBlocking(
        String action,
        TransportRequest transportRequest,
        User fallbackUser,
        Consumer<Tuple<Authentication, String>> verifier
    ) {
        SetOnce<String> reqId = new SetOnce<>();
        PlainActionFuture<Authentication> future = new PlainActionFuture<>() {
            @Override
            public void onResponse(Authentication result) {
                reqId.set(expectAuditRequestId(threadContext));
                assertThat(new AuthenticationContextSerializer().getAuthentication(threadContext), is(result));
                if (verifier != null) {
                    verifier.accept(new Tuple<>(result, reqId.get()));
                }
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
        future.actionGet();
        assertThat(expectAuditRequestId(threadContext), is(reqId.get()));
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

        private final List<Realm> allRealms;
        private final List<Realm> internalRealms;

        TestRealms(
            Settings settings,
            Environment env,
            Map<String, Factory> factories,
            XPackLicenseState licenseState,
            ThreadContext threadContext,
            ReservedRealm reservedRealm,
            List<Realm> realms,
            List<Realm> internalRealms
        ) throws Exception {
            super(settings, env, factories, licenseState, threadContext, reservedRealm);
            this.allRealms = realms;
            this.internalRealms = internalRealms;
        }

        @Override
        protected List<Realm> calculateLicensedRealms(XPackLicenseState licenseState) {
            if (allRealms == null) {
                // This can happen because the realms are recalculated during construction
                return super.calculateLicensedRealms(licenseState);
            }

            // Use custom as a placeholder for all non-internal realm
            if (Security.CUSTOM_REALMS_FEATURE.checkWithoutTracking(licenseState)) {
                return allRealms;
            } else {
                return internalRealms;
            }
        }

        // Make public for testing
        public void recomputeActiveRealms() {
            super.recomputeActiveRealms();
        }

        @Override
        protected void handleDisabledRealmDueToLicenseChange(Realm realm, XPackLicenseState licenseStateSnapshot) {
            // Ignore
        }
    }

    private void logAndFail(Exception e) {
        logger.error("unexpected exception", e);
        fail("unexpected exception " + e.getMessage());
    }

    private void setCompletedToTrue(AtomicBoolean completed) {
        assertTrue(completed.compareAndSet(false, true));
    }

    private SecurityIndexManager.IndexState dummyState(ClusterHealthStatus indexStatus) {

        return this.securityIndex.new IndexState(
            Metadata.DEFAULT_PROJECT_ID, SecurityIndexManager.ProjectStatus.PROJECT_AVAILABLE, Instant.now(), true, true, true, true, true,
            null, null, null, null, concreteSecurityIndexName, indexStatus, IndexMetadata.State.OPEN, "my_uuid", Set.of()
        );
    }

    @SuppressWarnings("unchecked")
    private static <T> Consumer<T> anyConsumer() {
        return any(Consumer.class);
    }
}
