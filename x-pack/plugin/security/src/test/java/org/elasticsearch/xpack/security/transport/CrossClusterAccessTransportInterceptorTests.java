/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.ssl.SslTrustConfig;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteConnectionManager;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.SslProfile;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.After;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomUniquelyNamedRoleDescriptors;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class CrossClusterAccessTransportInterceptorTests extends AbstractServerTransportInterceptorTests {

    private Settings settings;
    private ThreadPool threadPool;
    private ThreadContext threadContext;
    private SecurityContext securityContext;
    private ClusterService clusterService;
    private MockLicenseState mockLicenseState;
    private DestructiveOperations destructiveOperations;
    private CrossClusterApiKeySignatureManager crossClusterApiKeySignatureManager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put("path.home", createTempDir()).build();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        threadContext = threadPool.getThreadContext();
        securityContext = spy(new SecurityContext(settings, threadPool.getThreadContext()));
        mockLicenseState = MockLicenseState.createMock();
        Mockito.when(mockLicenseState.isAllowed(Security.ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE)).thenReturn(true);
        destructiveOperations = new DestructiveOperations(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
        );
        crossClusterApiKeySignatureManager = mock(CrossClusterApiKeySignatureManager.class);
    }

    @After
    public void stopThreadPool() throws Exception {
        clusterService.close();
        terminate(threadPool);
    }

    public void testSendWithCrossClusterAccessHeadersForSystemUserRegularAction() throws Exception {
        final String action;
        final TransportRequest request;
        if (randomBoolean()) {
            action = randomAlphaOfLengthBetween(5, 30);
            request = mock(TransportRequest.class);
        } else {
            action = ClusterStateAction.NAME;
            request = mock(ClusterStateRequest.class);
        }
        doTestSendWithCrossClusterAccessHeaders(
            true,
            action,
            request,
            AuthenticationTestHelper.builder().internal(InternalUsers.SYSTEM_USER).build()
        );
    }

    public void testSendWithCrossClusterAccessHeadersForSystemUserCcrInternalAction() throws Exception {
        final String action = randomFrom(
            "internal:admin/ccr/restore/session/put",
            "internal:admin/ccr/restore/session/clear",
            "internal:admin/ccr/restore/file_chunk/get"
        );
        final TransportRequest request = mock(TransportRequest.class);
        doTestSendWithCrossClusterAccessHeaders(
            true,
            action,
            request,
            AuthenticationTestHelper.builder().internal(InternalUsers.SYSTEM_USER).build()
        );
    }

    public void testSendWithCrossClusterAccessHeadersForRegularUserRegularAction() throws Exception {
        final Authentication authentication = randomValueOtherThanMany(
            authc -> authc.getAuthenticationType() == Authentication.AuthenticationType.INTERNAL,
            () -> AuthenticationTestHelper.builder().build()
        );
        final String action = randomAlphaOfLengthBetween(5, 30);
        final TransportRequest request = mock(TransportRequest.class);
        doTestSendWithCrossClusterAccessHeaders(false, action, request, authentication);
    }

    public void testSendWithCrossClusterAccessHeadersForRegularUserClusterStateAction() throws Exception {
        final Authentication authentication = randomValueOtherThanMany(
            authc -> authc.getAuthenticationType() == Authentication.AuthenticationType.INTERNAL,
            () -> AuthenticationTestHelper.builder().build()
        );
        final String action = ClusterStateAction.NAME;
        final TransportRequest request = mock(ClusterStateRequest.class);
        doTestSendWithCrossClusterAccessHeaders(true, action, request, authentication);
    }

    private void doTestSendWithCrossClusterAccessHeaders(
        boolean shouldAssertForSystemUser,
        String action,
        TransportRequest request,
        Authentication authentication
    ) throws IOException {
        doTestSendWithCrossClusterAccessHeaders(shouldAssertForSystemUser, action, request, authentication, TransportVersion.current());
    }

    private void doTestSendWithCrossClusterAccessHeaders(
        boolean shouldAssertForSystemUser,
        String action,
        TransportRequest request,
        Authentication authentication,
        TransportVersion transportVersion
    ) throws IOException {
        authentication.writeToContext(threadContext);
        final String expectedRequestId = AuditUtil.getOrGenerateRequestId(threadContext);
        if (randomBoolean()) {
            threadContext.putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, randomProjectIdOrDefault().id());
        }
        final String remoteClusterAlias = randomAlphaOfLengthBetween(5, 10);
        final String encodedApiKey = randomAlphaOfLengthBetween(10, 42);
        final String remoteClusterCredential = ApiKeyService.withApiKeyPrefix(encodedApiKey);
        final AuthorizationService authzService = mock(AuthorizationService.class);
        // We capture the listener so that we can complete the full flow, by calling onResponse further down
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<ActionListener<RoleDescriptorsIntersection>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doAnswer(i -> null).when(authzService)
            .getRoleDescriptorsIntersectionForRemoteCluster(any(), any(), any(), listenerCaptor.capture());

        final SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            authzService,
            mockSslService(),
            securityContext,
            destructiveOperations,
            new CrossClusterAccessTransportInterceptor(
                settings,
                threadPool,
                mock(AuthenticationService.class),
                authzService,
                securityContext,
                mock(CrossClusterAccessAuthenticationService.class),
                crossClusterApiKeySignatureManager,
                mockLicenseState,
                ignored -> Optional.of(
                    new RemoteConnectionManager.RemoteClusterAliasWithCredentials(
                        remoteClusterAlias,
                        new SecureString(encodedApiKey.toCharArray())
                    )
                )
            )
        );

        final AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        final AtomicReference<String> sentAction = new AtomicReference<>();
        final AtomicReference<String> sentCredential = new AtomicReference<>();
        final AtomicReference<CrossClusterAccessSubjectInfo> sentCrossClusterAccessSubjectInfo = new AtomicReference<>();
        final TransportInterceptor.AsyncSender sender = interceptor.interceptSender(new TransportInterceptor.AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                if (calledWrappedSender.compareAndSet(false, true) == false) {
                    fail("sender called more than once");
                }
                assertThat(securityContext.getAuthentication(), nullValue());
                assertThat(AuditUtil.extractRequestId(securityContext.getThreadContext()), equalTo(expectedRequestId));
                assertThat(threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), nullValue());
                sentAction.set(action);
                sentCredential.set(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY));
                try {
                    sentCrossClusterAccessSubjectInfo.set(
                        CrossClusterAccessSubjectInfo.decode(
                            securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY)
                        )
                    );
                } catch (IOException e) {
                    fail("no exceptions expected but got " + e);
                }
                handler.handleResponse(null);
            }
        });
        final Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(transportVersion);

        sender.sendRequest(connection, action, request, null, new TransportResponseHandler<>() {
            @Override
            public Executor executor() {
                return TransportResponseHandler.TRANSPORT_WORKER;
            }

            @Override
            public void handleResponse(TransportResponse response) {
                // Headers should get restored before handle response is called
                assertThat(securityContext.getAuthentication(), equalTo(authentication));
                assertThat(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY), nullValue());
                assertThat(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY), nullValue());
            }

            @Override
            public void handleException(TransportException exp) {
                fail("no exceptions expected but got " + exp);
            }

            @Override
            public TransportResponse read(StreamInput in) {
                return null;
            }
        });
        if (shouldAssertForSystemUser) {
            assertThat(
                sentCrossClusterAccessSubjectInfo.get(),
                equalTo(
                    SystemUser.crossClusterAccessSubjectInfo(
                        authentication.getEffectiveSubject().getTransportVersion(),
                        authentication.getEffectiveSubject().getRealm().getNodeName()
                    )
                )
            );
            verify(authzService, never()).getRoleDescriptorsIntersectionForRemoteCluster(
                eq(remoteClusterAlias),
                eq(TransportVersion.current()),
                eq(authentication.getEffectiveSubject()),
                anyActionListener()
            );
        } else {
            final RoleDescriptorsIntersection expectedRoleDescriptorsIntersection = new RoleDescriptorsIntersection(
                randomList(1, 3, () -> Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 1)))
            );
            // Call listener to complete flow
            listenerCaptor.getValue().onResponse(expectedRoleDescriptorsIntersection);
            verify(authzService, times(1)).getRoleDescriptorsIntersectionForRemoteCluster(
                eq(remoteClusterAlias),
                eq(TransportVersion.current()),
                eq(authentication.getEffectiveSubject()),
                anyActionListener()
            );
            assertThat(
                sentCrossClusterAccessSubjectInfo.get(),
                equalTo(new CrossClusterAccessSubjectInfo(authentication, expectedRoleDescriptorsIntersection))
            );
        }
        assertTrue(calledWrappedSender.get());
        if (action.startsWith("internal:")) {
            assertThat(sentAction.get(), equalTo("indices:internal/" + action.substring("internal:".length())));
        } else {
            assertThat(sentAction.get(), equalTo(action));
        }
        assertThat(sentCredential.get(), equalTo(remoteClusterCredential));
        verify(securityContext, never()).executeAsInternalUser(any(), any(), anyConsumer());
        assertThat(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY), nullValue());
        assertThat(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY), nullValue());
        assertThat(AuditUtil.extractRequestId(securityContext.getThreadContext()), equalTo(expectedRequestId));
    }

    public void testSendWithUserIfCrossClusterAccessHeadersConditionNotMet() throws Exception {
        boolean noCredential = randomBoolean();
        final boolean notRemoteConnection = randomBoolean();
        // Ensure at least one condition fails
        if (false == (notRemoteConnection || noCredential)) {
            noCredential = true;
        }
        final boolean finalNoCredential = noCredential;
        final String remoteClusterAlias = randomAlphaOfLengthBetween(5, 10);
        final String encodedApiKey = randomAlphaOfLengthBetween(10, 42);
        final AuthenticationTestHelper.AuthenticationTestBuilder builder = AuthenticationTestHelper.builder();
        final Authentication authentication = randomFrom(
            builder.apiKey().build(),
            builder.serviceAccount().build(),
            builder.user(new User(randomAlphaOfLengthBetween(3, 10), randomRoles())).realm().build()
        );
        authentication.writeToContext(threadContext);

        final AuthorizationService authzService = mock(AuthorizationService.class);
        final SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            authzService,
            mockSslService(),
            securityContext,
            destructiveOperations,
            new CrossClusterAccessTransportInterceptor(
                settings,
                threadPool,
                mock(AuthenticationService.class),
                authzService,
                securityContext,
                mock(CrossClusterAccessAuthenticationService.class),
                crossClusterApiKeySignatureManager,
                mockLicenseState,
                ignored -> notRemoteConnection
                    ? Optional.empty()
                    : (finalNoCredential
                        ? Optional.of(new RemoteConnectionManager.RemoteClusterAliasWithCredentials(remoteClusterAlias, null))
                        : Optional.of(
                            new RemoteConnectionManager.RemoteClusterAliasWithCredentials(
                                remoteClusterAlias,
                                new SecureString(encodedApiKey.toCharArray())
                            )
                        ))
            )
        );

        final AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        final AtomicReference<Authentication> sentAuthentication = new AtomicReference<>();
        final TransportInterceptor.AsyncSender sender = interceptor.interceptSender(new TransportInterceptor.AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                if (calledWrappedSender.compareAndSet(false, true) == false) {
                    fail("sender called more than once");
                }
                sentAuthentication.set(securityContext.getAuthentication());
                assertThat(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY), nullValue());
                assertThat(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY), nullValue());
            }
        });
        final Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(TransportVersion.current());
        sender.sendRequest(connection, "action", mock(TransportRequest.class), null, null);
        assertTrue(calledWrappedSender.get());
        assertThat(sentAuthentication.get(), equalTo(authentication));
        verify(authzService, never()).getRoleDescriptorsIntersectionForRemoteCluster(any(), any(), any(), anyActionListener());
        assertThat(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY), nullValue());
        assertThat(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY), nullValue());
    }

    public void testSendRemoteRequestFailsIfUserHasNoRemoteIndicesPrivileges() throws Exception {
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User(randomAlphaOfLengthBetween(3, 10), randomRoles()))
            .realm()
            .build();
        authentication.writeToContext(threadContext);
        final String remoteClusterAlias = randomAlphaOfLengthBetween(5, 10);
        final String encodedApiKey = randomAlphaOfLengthBetween(10, 42);
        final String remoteClusterCredential = ApiKeyService.withApiKeyPrefix(encodedApiKey);
        final AuthorizationService authzService = mock(AuthorizationService.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final var listener = (ActionListener<RoleDescriptorsIntersection>) invocation.getArgument(3);
            listener.onResponse(RoleDescriptorsIntersection.EMPTY);
            return null;
        }).when(authzService).getRoleDescriptorsIntersectionForRemoteCluster(any(), any(), any(), anyActionListener());

        final SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            authzService,
            mockSslService(),
            securityContext,
            destructiveOperations,
            new CrossClusterAccessTransportInterceptor(
                settings,
                threadPool,
                mock(AuthenticationService.class),
                authzService,
                securityContext,
                mock(CrossClusterAccessAuthenticationService.class),
                crossClusterApiKeySignatureManager,
                mockLicenseState,
                ignored -> Optional.of(
                    new RemoteConnectionManager.RemoteClusterAliasWithCredentials(
                        remoteClusterAlias,
                        new SecureString(encodedApiKey.toCharArray())
                    )
                )
            )
        );

        final TransportInterceptor.AsyncSender sender = interceptor.interceptSender(new TransportInterceptor.AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                fail("request should have failed");
            }
        });
        final Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(TransportVersion.current());

        final ElasticsearchSecurityException expectedException = new ElasticsearchSecurityException("remote action denied");
        when(authzService.remoteActionDenied(authentication, "action", remoteClusterAlias)).thenReturn(expectedException);

        final var actualException = new AtomicReference<Throwable>();
        sender.sendRequest(connection, "action", mock(TransportRequest.class), null, new TransportResponseHandler<>() {
            @Override
            public Executor executor() {
                return TransportResponseHandler.TRANSPORT_WORKER;
            }

            @Override
            public void handleResponse(TransportResponse response) {
                fail("should not success");
            }

            @Override
            public void handleException(TransportException exp) {
                actualException.set(exp.getCause());
            }

            @Override
            public TransportResponse read(StreamInput in) {
                return null;
            }
        });
        assertThat(actualException.get(), is(expectedException));
        assertThat(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY), nullValue());
        assertThat(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY), nullValue());
    }

    public void testSendWithCrossClusterAccessHeadersWithUnsupportedLicense() throws Exception {
        final MockLicenseState unsupportedLicenseState = MockLicenseState.createMock();
        Mockito.when(unsupportedLicenseState.isAllowed(Security.ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE)).thenReturn(false);

        AuthenticationTestHelper.builder().build().writeToContext(threadContext);
        final String remoteClusterAlias = randomAlphaOfLengthBetween(5, 10);

        final SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mockSslService(),
            securityContext,
            destructiveOperations,
            new CrossClusterAccessTransportInterceptor(
                settings,
                threadPool,
                mock(AuthenticationService.class),
                mock(AuthorizationService.class),
                securityContext,
                mock(CrossClusterAccessAuthenticationService.class),
                crossClusterApiKeySignatureManager,
                unsupportedLicenseState,
                mockRemoteClusterCredentialsResolver(remoteClusterAlias)
            )
        );

        final TransportInterceptor.AsyncSender sender = interceptor.interceptSender(
            mock(TransportInterceptor.AsyncSender.class, ignored -> {
                throw new AssertionError("sender should not be called");
            })
        );
        final Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(TransportVersion.current());
        final AtomicBoolean calledHandleException = new AtomicBoolean(false);
        final AtomicReference<TransportException> actualException = new AtomicReference<>();
        sender.sendRequest(connection, "action", mock(TransportRequest.class), null, new TransportResponseHandler<>() {
            @Override
            public Executor executor() {
                return TransportResponseHandler.TRANSPORT_WORKER;
            }

            @Override
            public void handleResponse(TransportResponse response) {
                fail("should not receive a response");
            }

            @Override
            public void handleException(TransportException exp) {
                if (calledHandleException.compareAndSet(false, true) == false) {
                    fail("handle exception called more than once");
                }
                actualException.set(exp);
            }

            @Override
            public TransportResponse read(StreamInput in) {
                fail("should not receive a response");
                return null;
            }
        });
        assertThat(actualException.get(), instanceOf(SendRequestTransportException.class));
        assertThat(actualException.get().getCause(), instanceOf(ElasticsearchSecurityException.class));
        assertThat(
            actualException.get().getCause().getMessage(),
            equalTo("current license is non-compliant for [" + Security.ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE.getName() + "]")
        );
        assertThat(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY), nullValue());
        assertThat(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY), nullValue());
    }

    public void testProfileFiltersCreatedDifferentlyForDifferentTransportAndRemoteClusterSslSettings() {
        // filters are created irrespective of ssl enabled
        final boolean transportSslEnabled = randomBoolean();
        final boolean remoteClusterSslEnabled = randomBoolean();
        final Settings.Builder builder = Settings.builder()
            .put(this.settings)
            .put("xpack.security.transport.ssl.enabled", transportSslEnabled)
            .put("remote_cluster_server.enabled", true)
            .put("xpack.security.remote_cluster_server.ssl.enabled", remoteClusterSslEnabled);
        if (randomBoolean()) {
            builder.put("xpack.security.remote_cluster_client.ssl.enabled", randomBoolean());  // client SSL won't be processed
        }

        final SslProfile defaultProfile = mock(SslProfile.class);
        when(defaultProfile.configuration()).thenReturn(
            new SslConfiguration(
                "xpack.security.transport.ssl",
                randomBoolean(),
                mock(SslTrustConfig.class),
                mock(SslKeyConfig.class),
                randomFrom(SslVerificationMode.values()),
                SslClientAuthenticationMode.REQUIRED,
                List.of("TLS_AES_256_GCM_SHA384"),
                List.of("TLSv1.3"),
                randomLongBetween(1, 100000)
            )
        );
        final SslProfile remoteProfile = mock(SslProfile.class);
        when(remoteProfile.configuration()).thenReturn(
            new SslConfiguration(
                "xpack.security.remote_cluster_server.ssl",
                randomBoolean(),
                mock(SslTrustConfig.class),
                mock(SslKeyConfig.class),
                randomFrom(SslVerificationMode.values()),
                SslClientAuthenticationMode.NONE,
                List.of(Runtime.version().feature() < 24 ? "TLS_RSA_WITH_AES_256_GCM_SHA384" : "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"),
                List.of("TLSv1.2"),
                randomLongBetween(1, 100000)
            )
        );

        final SSLService sslService = mock(SSLService.class);
        when(sslService.profile("xpack.security.transport.ssl.")).thenReturn(defaultProfile);

        when(sslService.profile("xpack.security.remote_cluster_server.ssl.")).thenReturn(remoteProfile);
        doThrow(new AssertionError("profile filters should not be configured for remote cluster client")).when(sslService)
            .profile("xpack.security.remote_cluster_client.ssl.");

        final AuthenticationService authcService = mock(AuthenticationService.class);
        final AuthorizationService authzService = mock(AuthorizationService.class);
        final var securityServerTransportInterceptor = new SecurityServerTransportInterceptor(
            builder.build(),
            threadPool,
            authcService,
            authzService,
            sslService,
            securityContext,
            destructiveOperations,
            new CrossClusterAccessTransportInterceptor(
                builder.build(),
                threadPool,
                authcService,
                authzService,
                securityContext,
                mock(CrossClusterAccessAuthenticationService.class),
                crossClusterApiKeySignatureManager,
                mockLicenseState
            )
        );

        final Map<String, ServerTransportFilter> profileFilters = securityServerTransportInterceptor.getProfileFilters();
        assertThat(profileFilters.keySet(), containsInAnyOrder("default", "_remote_cluster"));
        assertThat(profileFilters.get("default").isExtractClientCert(), is(transportSslEnabled));
        assertThat(profileFilters.get("default"), not(instanceOf(CrossClusterAccessServerTransportFilter.class)));
        assertThat(profileFilters.get("_remote_cluster").isExtractClientCert(), is(false));
        assertThat(profileFilters.get("_remote_cluster"), instanceOf(CrossClusterAccessServerTransportFilter.class));
    }

    public void testNoProfileFilterForRemoteClusterWhenTheFeatureIsDisabled() {
        final boolean transportSslEnabled = randomBoolean();
        final Settings.Builder builder = Settings.builder()
            .put(this.settings)
            .put("xpack.security.transport.ssl.enabled", transportSslEnabled)
            .put("remote_cluster_server.enabled", false)
            .put("xpack.security.remote_cluster_server.ssl.enabled", randomBoolean());
        if (randomBoolean()) {
            builder.put("xpack.security.remote_cluster_client.ssl.enabled", randomBoolean());  // client SSL won't be processed
        }

        final SslProfile profile = mock(SslProfile.class);
        when(profile.configuration()).thenReturn(
            new SslConfiguration(
                "xpack.security.transport.ssl",
                randomBoolean(),
                mock(SslTrustConfig.class),
                mock(SslKeyConfig.class),
                randomFrom(SslVerificationMode.values()),
                SslClientAuthenticationMode.REQUIRED,
                List.of("TLS_AES_256_GCM_SHA384"),
                List.of("TLSv1.3"),
                randomLongBetween(1, 100000)
            )
        );

        final SSLService sslService = mock(SSLService.class);
        when(sslService.profile("xpack.security.transport.ssl.")).thenReturn(profile);

        doThrow(new AssertionError("profile filters should not be configured for remote cluster server when the port is disabled")).when(
            sslService
        ).profile("xpack.security.remote_cluster_server.ssl.");
        doThrow(new AssertionError("profile filters should not be configured for remote cluster client")).when(sslService)
            .profile("xpack.security.remote_cluster_client.ssl.");

        final var securityServerTransportInterceptor = new SecurityServerTransportInterceptor(
            builder.build(),
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            sslService,
            securityContext,
            destructiveOperations,
            new CrossClusterAccessTransportInterceptor(
                builder.build(),
                threadPool,
                mock(AuthenticationService.class),
                mock(AuthorizationService.class),
                securityContext,
                mock(CrossClusterAccessAuthenticationService.class),
                crossClusterApiKeySignatureManager,
                mockLicenseState
            )
        );

        final Map<String, ServerTransportFilter> profileFilters = securityServerTransportInterceptor.getProfileFilters();
        assertThat(profileFilters.keySet(), contains("default"));
        assertThat(profileFilters.get("default").isExtractClientCert(), is(transportSslEnabled));
    }

    public void testGetRemoteProfileTransportFilter() {
        final boolean remoteClusterSslEnabled = randomBoolean();
        final Settings.Builder builder = Settings.builder()
            .put(this.settings)
            .put("remote_cluster_server.enabled", true)
            .put("xpack.security.remote_cluster_server.ssl.enabled", remoteClusterSslEnabled);
        if (randomBoolean()) {
            builder.put("xpack.security.remote_cluster_client.ssl.enabled", randomBoolean());  // client SSL won't be processed
        }

        final SslProfile remoteProfile = mock(SslProfile.class);
        when(remoteProfile.configuration()).thenReturn(
            new SslConfiguration(
                "xpack.security.remote_cluster_server.ssl",
                randomBoolean(),
                mock(SslTrustConfig.class),
                mock(SslKeyConfig.class),
                randomFrom(SslVerificationMode.values()),
                SslClientAuthenticationMode.NONE,
                List.of(Runtime.version().feature() < 24 ? "TLS_RSA_WITH_AES_256_GCM_SHA384" : "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"),
                List.of("TLSv1.2"),
                randomLongBetween(1, 100000)
            )
        );

        final SSLService sslService = mock(SSLService.class);
        when(sslService.profile("xpack.security.remote_cluster_server.ssl.")).thenReturn(remoteProfile);
        doThrow(new AssertionError("profile filters should not be configured for remote cluster client")).when(sslService)
            .profile("xpack.security.remote_cluster_client.ssl.");

        final AuthenticationService authcService = mock(AuthenticationService.class);
        final AuthorizationService authzService = mock(AuthorizationService.class);
        CrossClusterAccessTransportInterceptor interceptor = new CrossClusterAccessTransportInterceptor(
            builder.build(),
            threadPool,
            authcService,
            authzService,
            securityContext,
            mock(CrossClusterAccessAuthenticationService.class),
            crossClusterApiKeySignatureManager,
            mockLicenseState
        );

        final Optional<ServerTransportFilter> remoteProfileTransportFilter = interceptor.getRemoteProfileTransportFilter(
            remoteProfile,
            destructiveOperations
        );
        assertThat(remoteProfileTransportFilter.isPresent(), is(true));
        assertThat(remoteProfileTransportFilter.get(), instanceOf(CrossClusterAccessServerTransportFilter.class));
    }

    public void testGetRemoteProfileTransportFilterWhenRemoteClusterServerIsDisabled() {
        final boolean remoteClusterSslEnabled = randomBoolean();
        final Settings.Builder builder = Settings.builder()
            .put(this.settings)
            .put("remote_cluster_server.enabled", false)
            .put("xpack.security.remote_cluster_server.ssl.enabled", remoteClusterSslEnabled);
        if (randomBoolean()) {
            builder.put("xpack.security.remote_cluster_client.ssl.enabled", randomBoolean());  // client SSL won't be processed
        }

        final SslProfile remoteProfile = mock(SslProfile.class);
        when(remoteProfile.configuration()).thenReturn(
            new SslConfiguration(
                "xpack.security.remote_cluster_server.ssl",
                randomBoolean(),
                mock(SslTrustConfig.class),
                mock(SslKeyConfig.class),
                randomFrom(SslVerificationMode.values()),
                SslClientAuthenticationMode.NONE,
                List.of(Runtime.version().feature() < 24 ? "TLS_RSA_WITH_AES_256_GCM_SHA384" : "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"),
                List.of("TLSv1.2"),
                randomLongBetween(1, 100000)
            )
        );

        final SSLService sslService = mock(SSLService.class);
        when(sslService.profile("xpack.security.remote_cluster_server.ssl.")).thenReturn(remoteProfile);
        doThrow(new AssertionError("profile filters should not be configured for remote cluster client")).when(sslService)
            .profile("xpack.security.remote_cluster_client.ssl.");

        final AuthenticationService authcService = mock(AuthenticationService.class);
        final AuthorizationService authzService = mock(AuthorizationService.class);
        CrossClusterAccessTransportInterceptor interceptor = new CrossClusterAccessTransportInterceptor(
            builder.build(),
            threadPool,
            authcService,
            authzService,
            securityContext,
            mock(CrossClusterAccessAuthenticationService.class),
            crossClusterApiKeySignatureManager,
            mockLicenseState
        );

        final Optional<ServerTransportFilter> remoteProfileTransportFilter = interceptor.getRemoteProfileTransportFilter(
            remoteProfile,
            destructiveOperations
        );
        assertThat(remoteProfileTransportFilter.isPresent(), is(false));
    }

    public void testSendWithCrossClusterApiKeySignatureSkippedOnUnsupportedConnection() throws Exception {
        final String action;
        final TransportRequest request;
        if (randomBoolean()) {
            action = randomAlphaOfLengthBetween(5, 30);
            request = mock(TransportRequest.class);
        } else {
            action = ClusterStateAction.NAME;
            request = mock(ClusterStateRequest.class);
        }

        var signer = mock(CrossClusterApiKeySignatureManager.Signer.class);
        when(crossClusterApiKeySignatureManager.signerForClusterAlias(anyString())).thenReturn(signer);
        var transportVersion = TransportVersionUtils.getPreviousVersion(
            CrossClusterAccessTransportInterceptor.ADD_CROSS_CLUSTER_API_KEY_SIGNATURE
        );
        doTestSendWithCrossClusterAccessHeaders(
            true,
            action,
            request,
            AuthenticationTestHelper.builder().internal(InternalUsers.SYSTEM_USER).transportVersion(transportVersion).build(),
            transportVersion
        );

        verifyNoInteractions(signer);
    }

    public void testSendWithCrossClusterApiKeySignatureSentOnSupportedConnection() throws Exception {
        final String action;
        final TransportRequest request;
        if (randomBoolean()) {
            action = randomAlphaOfLengthBetween(5, 30);
            request = mock(TransportRequest.class);
        } else {
            action = ClusterStateAction.NAME;
            request = mock(ClusterStateRequest.class);
        }

        var testSignature = getTestSignature();
        var signer = mock(CrossClusterApiKeySignatureManager.Signer.class);
        when(signer.sign(anyString(), anyString())).thenReturn(testSignature);
        when(crossClusterApiKeySignatureManager.signerForClusterAlias(anyString())).thenReturn(signer);

        var transportVersion = CrossClusterAccessTransportInterceptor.ADD_CROSS_CLUSTER_API_KEY_SIGNATURE;

        doTestSendWithCrossClusterAccessHeaders(
            true,
            action,
            request,
            AuthenticationTestHelper.builder().internal(InternalUsers.SYSTEM_USER).transportVersion(transportVersion).build(),
            transportVersion
        );

        verify(signer, times(1)).sign(anyString(), anyString());
    }

    private X509CertificateSignature getTestSignature() throws CertificateException, IOException {
        return new X509CertificateSignature(getTestCertificates(), "SHA256withRSA", new BytesArray(new byte[] { 1, 2, 3, 4 }));
    }

    private X509Certificate[] getTestCertificates() throws CertificateException, IOException {
        return PemUtils.readCertificates(List.of(getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.crt")))
            .stream()
            .map(cert -> (X509Certificate) cert)
            .toArray(X509Certificate[]::new);
    }

}
