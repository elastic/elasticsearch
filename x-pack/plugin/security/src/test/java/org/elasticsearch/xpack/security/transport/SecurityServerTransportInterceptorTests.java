/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.ssl.SslTrustConfig;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.RemoteConnectionManager.RemoteClusterAliasWithCredentials;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor.AsyncSender;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.DATA_STREAM_LIFECYCLE_ORIGIN;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_PROFILE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomUniquelyNamedRoleDescriptors;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SecurityServerTransportInterceptorTests extends ESTestCase {

    private Settings settings;
    private ThreadPool threadPool;
    private ThreadContext threadContext;
    private SecurityContext securityContext;
    private ClusterService clusterService;
    private MockLicenseState mockLicenseState;

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
    }

    @After
    public void stopThreadPool() throws Exception {
        clusterService.close();
        terminate(threadPool);
    }

    public void testSendAsync() throws Exception {
        final User user = new User("test", randomRoles());
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new RealmRef("ldap", "foo", "node1"))
            .build(false);
        authentication.writeToContext(threadContext);
        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mock(SSLService.class),
            securityContext,
            new DestructiveOperations(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
            ),
            mock(CrossClusterAccessAuthenticationService.class),
            mockLicenseState
        );
        ClusterServiceUtils.setState(clusterService, clusterService.state()); // force state update to trigger listener

        AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        AtomicReference<User> sendingUser = new AtomicReference<>();
        AsyncSender sender = interceptor.interceptSender(new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                if (calledWrappedSender.compareAndSet(false, true) == false) {
                    fail("sender called more than once!");
                }
                sendingUser.set(securityContext.getUser());
            }
        });
        Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(TransportVersion.current());
        sender.sendRequest(connection, "indices:foo", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertEquals(user, sendingUser.get());
        assertEquals(user, securityContext.getUser());
        verify(securityContext, never()).executeAsInternalUser(any(InternalUser.class), any(TransportVersion.class), anyConsumer());
    }

    public void testSendAsyncSwitchToSystem() throws Exception {
        final User user = new User("test", randomRoles());
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new RealmRef("ldap", "foo", "node1"))
            .build(false);
        authentication.writeToContext(threadContext);
        threadContext.putTransient(AuthorizationServiceField.ORIGINATING_ACTION_KEY, "indices:foo");

        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mock(SSLService.class),
            securityContext,
            new DestructiveOperations(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
            ),
            mock(CrossClusterAccessAuthenticationService.class),
            mockLicenseState
        );
        ClusterServiceUtils.setState(clusterService, clusterService.state()); // force state update to trigger listener

        AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        AtomicReference<User> sendingUser = new AtomicReference<>();
        AsyncSender sender = interceptor.interceptSender(new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                if (calledWrappedSender.compareAndSet(false, true) == false) {
                    fail("sender called more than once!");
                }
                sendingUser.set(securityContext.getUser());
            }
        });
        Connection connection = mock(Connection.class);
        when(connection.getTransportVersion()).thenReturn(TransportVersion.current());
        sender.sendRequest(connection, "internal:foo", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertNotEquals(user, sendingUser.get());
        assertEquals(InternalUsers.SYSTEM_USER, sendingUser.get());
        assertEquals(user, securityContext.getUser());
        verify(securityContext).executeAsInternalUser(any(InternalUser.class), eq(TransportVersion.current()), anyConsumer());
    }

    public void testSendWithoutUser() throws Exception {
        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mock(SSLService.class),
            securityContext,
            new DestructiveOperations(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
            ),
            mock(CrossClusterAccessAuthenticationService.class),
            mockLicenseState
        ) {
            @Override
            void assertNoAuthentication(String action) {}
        };
        ClusterServiceUtils.setState(clusterService, clusterService.state()); // force state update to trigger listener

        assertNull(securityContext.getUser());
        AsyncSender sender = interceptor.interceptSender(new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                fail("sender should not be called!");
            }
        });
        Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(TransportVersion.current());
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> sender.sendRequest(connection, "indices:foo", null, null, null)
        );
        assertEquals("there should always be a user when sending a message for action [indices:foo]", e.getMessage());
        assertNull(securityContext.getUser());
        verify(securityContext, never()).executeAsInternalUser(any(InternalUser.class), any(TransportVersion.class), anyConsumer());
    }

    public void testSendToNewerVersionSetsCorrectVersion() throws Exception {
        final User authUser = randomBoolean() ? new User("authenticator") : null;
        final Authentication authentication;
        if (authUser == null) {
            authentication = AuthenticationTestHelper.builder()
                .user(new User("joe", randomRoles()))
                .realmRef(new RealmRef("file", "file", "node1"))
                .build(false);
        } else {
            authentication = AuthenticationTestHelper.builder()
                .realm()
                .user(authUser)
                .realmRef(new RealmRef("file", "file", "node1"))
                .build(false)
                .runAs(new User("joe", randomRoles()), null);
        }
        authentication.writeToContext(threadContext);
        threadContext.putTransient(AuthorizationServiceField.ORIGINATING_ACTION_KEY, "indices:foo");

        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mock(SSLService.class),
            securityContext,
            new DestructiveOperations(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
            ),
            mock(CrossClusterAccessAuthenticationService.class),
            mockLicenseState
        );
        ClusterServiceUtils.setState(clusterService, clusterService.state()); // force state update to trigger listener

        AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        AtomicReference<User> sendingUser = new AtomicReference<>();
        AtomicReference<Authentication> authRef = new AtomicReference<>();
        AsyncSender intercepted = new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                if (calledWrappedSender.compareAndSet(false, true) == false) {
                    fail("sender called more than once!");
                }
                sendingUser.set(securityContext.getUser());
                authRef.set(securityContext.getAuthentication());
            }
        };
        AsyncSender sender = interceptor.interceptSender(intercepted);
        final TransportVersion connectionVersion = TransportVersion.fromId(TransportVersion.current().id() + randomIntBetween(100, 100000));
        assertEquals(TransportVersion.current(), TransportVersion.min(connectionVersion, TransportVersion.current()));

        Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(connectionVersion);
        sender.sendRequest(connection, "indices:foo[s]", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertEquals(authentication.getEffectiveSubject().getUser(), sendingUser.get());
        assertEquals(authentication.getEffectiveSubject().getUser(), securityContext.getUser());
        assertEquals(TransportVersion.current(), authRef.get().getEffectiveSubject().getTransportVersion());
        assertEquals(TransportVersion.current(), authentication.getEffectiveSubject().getTransportVersion());
    }

    public void testSendToOlderVersionSetsCorrectVersion() throws Exception {
        final User authUser = randomBoolean() ? new User("authenticator") : null;
        final Authentication authentication;
        if (authUser == null) {
            authentication = AuthenticationTestHelper.builder()
                .user(new User("joe", randomRoles()))
                .realmRef(new RealmRef("file", "file", "node1"))
                .build(false);
        } else {
            authentication = AuthenticationTestHelper.builder()
                .realm()
                .user(authUser)
                .realmRef(new RealmRef("file", "file", "node1"))
                .build(false)
                .runAs(new User("joe", randomRoles()), null);
        }
        authentication.writeToContext(threadContext);
        threadContext.putTransient(AuthorizationServiceField.ORIGINATING_ACTION_KEY, "indices:foo");

        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mock(SSLService.class),
            securityContext,
            new DestructiveOperations(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
            ),
            mock(CrossClusterAccessAuthenticationService.class),
            mockLicenseState
        );
        ClusterServiceUtils.setState(clusterService, clusterService.state()); // force state update to trigger listener

        AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        AtomicReference<User> sendingUser = new AtomicReference<>();
        AtomicReference<Authentication> authRef = new AtomicReference<>();
        AsyncSender intercepted = new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                if (calledWrappedSender.compareAndSet(false, true) == false) {
                    fail("sender called more than once!");
                }
                sendingUser.set(securityContext.getUser());
                authRef.set(securityContext.getAuthentication());
            }
        };
        AsyncSender sender = interceptor.interceptSender(intercepted);
        final TransportVersion connectionVersion = TransportVersion.fromId(TransportVersion.current().id() - randomIntBetween(100, 100000));
        assertEquals(connectionVersion, TransportVersion.min(connectionVersion, TransportVersion.current()));

        Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(connectionVersion);
        sender.sendRequest(connection, "indices:foo[s]", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertEquals(authentication.getEffectiveSubject().getUser(), sendingUser.get());
        assertEquals(authentication.getEffectiveSubject().getUser(), securityContext.getUser());
        assertEquals(connectionVersion, authRef.get().getEffectiveSubject().getTransportVersion());
        assertEquals(TransportVersion.current(), authentication.getEffectiveSubject().getTransportVersion());
    }

    public void testSetUserBasedOnActionOrigin() {
        final Map<String, User> originToUserMap = Map.of(
            SECURITY_ORIGIN,
            InternalUsers.XPACK_SECURITY_USER,
            SECURITY_PROFILE_ORIGIN,
            InternalUsers.SECURITY_PROFILE_USER,
            TRANSFORM_ORIGIN,
            InternalUsers.XPACK_USER,
            ASYNC_SEARCH_ORIGIN,
            InternalUsers.ASYNC_SEARCH_USER,
            DATA_STREAM_LIFECYCLE_ORIGIN,
            InternalUsers.DATA_STREAM_LIFECYCLE_USER
        );

        final String origin = randomFrom(originToUserMap.keySet());

        threadContext.putTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME, origin);
        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mock(SSLService.class),
            securityContext,
            new DestructiveOperations(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
            ),
            mock(CrossClusterAccessAuthenticationService.class),
            mockLicenseState
        );

        final AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        final AtomicReference<Authentication> authenticationRef = new AtomicReference<>();
        final AsyncSender intercepted = new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                if (calledWrappedSender.compareAndSet(false, true) == false) {
                    fail("sender called more than once!");
                }
                authenticationRef.set(securityContext.getAuthentication());
            }
        };
        final AsyncSender sender = interceptor.interceptSender(intercepted);

        Transport.Connection connection = mock(Transport.Connection.class);
        final TransportVersion connectionVersion = TransportVersionUtils.randomCompatibleVersion(random());
        when(connection.getTransportVersion()).thenReturn(connectionVersion);

        sender.sendRequest(connection, "indices:foo[s]", null, null, null);
        assertThat(calledWrappedSender.get(), is(true));
        final Authentication authentication = authenticationRef.get();
        assertThat(authentication, notNullValue());
        assertThat(authentication.getEffectiveSubject().getUser(), equalTo(originToUserMap.get(origin)));
        assertThat(authentication.getEffectiveSubject().getTransportVersion(), equalTo(connectionVersion));
    }

    public void testContextRestoreResponseHandler() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        threadContext.putTransient("foo", "bar");
        threadContext.putHeader("key", "value");
        try (ThreadContext.StoredContext storedContext = threadContext.stashContext()) {
            threadContext.putTransient("foo", "different_bar");
            threadContext.putHeader("key", "value2");
            TransportResponseHandler<Empty> handler = new TransportService.ContextRestoreResponseHandler<>(
                threadContext.wrapRestorable(storedContext),
                new TransportResponseHandler.Empty() {
                    @Override
                    public Executor executor() {
                        return TransportResponseHandler.TRANSPORT_WORKER;
                    }

                    @Override
                    public void handleResponse() {
                        assertEquals("bar", threadContext.getTransient("foo"));
                        assertEquals("value", threadContext.getHeader("key"));
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assertEquals("bar", threadContext.getTransient("foo"));
                        assertEquals("value", threadContext.getHeader("key"));
                    }
                }
            );

            handler.handleResponse(null);
            handler.handleException(null);
        }
    }

    public void testContextRestoreResponseHandlerRestoreOriginalContext() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient("foo", "bar");
        threadContext.putHeader("key", "value");
        TransportResponseHandler<Empty> handler;
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putTransient("foo", "different_bar");
            threadContext.putHeader("key", "value2");
            handler = new TransportService.ContextRestoreResponseHandler<>(
                threadContext.newRestorableContext(true),
                new TransportResponseHandler.Empty() {
                    @Override
                    public Executor executor() {
                        return TransportResponseHandler.TRANSPORT_WORKER;
                    }

                    @Override
                    public void handleResponse() {
                        assertEquals("different_bar", threadContext.getTransient("foo"));
                        assertEquals("value2", threadContext.getHeader("key"));
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assertEquals("different_bar", threadContext.getTransient("foo"));
                        assertEquals("value2", threadContext.getHeader("key"));
                    }
                }
            );
        }

        assertEquals("bar", threadContext.getTransient("foo"));
        assertEquals("value", threadContext.getHeader("key"));
        handler.handleResponse(null);

        assertEquals("bar", threadContext.getTransient("foo"));
        assertEquals("value", threadContext.getHeader("key"));
        handler.handleException(null);

        assertEquals("bar", threadContext.getTransient("foo"));
        assertEquals("value", threadContext.getHeader("key"));
    }

    public void testProfileSecuredRequestHandlerDecrementsRefCountOnFailure() throws IOException {
        final String profileName = "some-profile";
        final DestructiveOperations destructiveOperations = new DestructiveOperations(Settings.EMPTY, clusterService.getClusterSettings());
        final SecurityServerTransportInterceptor.ProfileSecuredRequestHandler<DeleteIndexRequest> requestHandler =
            new SecurityServerTransportInterceptor.ProfileSecuredRequestHandler<>(
                logger,
                TransportDeleteIndexAction.TYPE.name(),
                randomBoolean(),
                randomExecutor(threadPool),
                (request, channel, task) -> fail("should fail at destructive operations check to trigger listener failure"),
                Map.of(
                    profileName,
                    new ServerTransportFilter(null, null, threadContext, randomBoolean(), destructiveOperations, securityContext)
                ),
                threadPool
            );
        final TransportChannel channel = mock(TransportChannel.class);
        when(channel.getProfileName()).thenReturn(profileName);
        final AtomicBoolean exceptionSent = new AtomicBoolean(false);
        doAnswer(invocationOnMock -> {
            assertTrue(exceptionSent.compareAndSet(false, true));
            return null;
        }).when(channel).sendResponse(any(Exception.class));
        final AtomicBoolean decRefCalled = new AtomicBoolean(false);
        final DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest() {
            @Override
            public boolean decRef() {
                assertTrue(decRefCalled.compareAndSet(false, true));
                return super.decRef();
            }
        };
        requestHandler.messageReceived(deleteIndexRequest, channel, mock(Task.class));
        assertTrue(decRefCalled.get());
        assertTrue(exceptionSent.get());
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
            mock(SSLService.class),
            securityContext,
            new DestructiveOperations(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
            ),
            mock(CrossClusterAccessAuthenticationService.class),
            unsupportedLicenseState,
            mockRemoteClusterCredentialsResolver(remoteClusterAlias)
        );

        final AsyncSender sender = interceptor.interceptSender(mock(AsyncSender.class, ignored -> {
            throw new AssertionError("sender should not be called");
        }));
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

    private Function<Connection, Optional<RemoteClusterAliasWithCredentials>> mockRemoteClusterCredentialsResolver(
        String remoteClusterAlias
    ) {
        return connection -> Optional.of(
            new RemoteClusterAliasWithCredentials(remoteClusterAlias, new SecureString(randomAlphaOfLengthBetween(10, 42).toCharArray()))
        );
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
        authentication.writeToContext(threadContext);
        final String expectedRequestId = AuditUtil.getOrGenerateRequestId(threadContext);
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
            mock(SSLService.class),
            securityContext,
            new DestructiveOperations(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
            ),
            mock(CrossClusterAccessAuthenticationService.class),
            mockLicenseState,
            ignored -> Optional.of(new RemoteClusterAliasWithCredentials(remoteClusterAlias, new SecureString(encodedApiKey.toCharArray())))
        );

        final AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        final AtomicReference<String> sentAction = new AtomicReference<>();
        final AtomicReference<String> sentCredential = new AtomicReference<>();
        final AtomicReference<CrossClusterAccessSubjectInfo> sentCrossClusterAccessSubjectInfo = new AtomicReference<>();
        final AsyncSender sender = interceptor.interceptSender(new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Connection connection,
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
                sentAction.set(action);
                sentCredential.set(securityContext.getThreadContext().getHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY));
                try {
                    sentCrossClusterAccessSubjectInfo.set(
                        CrossClusterAccessSubjectInfo.readFromContext(securityContext.getThreadContext())
                    );
                } catch (IOException e) {
                    fail("no exceptions expected but got " + e);
                }
                handler.handleResponse(null);
            }
        });
        final Connection connection = mock(Connection.class);
        when(connection.getTransportVersion()).thenReturn(TransportVersion.current());

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
            mock(SSLService.class),
            securityContext,
            new DestructiveOperations(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
            ),
            mock(CrossClusterAccessAuthenticationService.class),
            mockLicenseState,
            ignored -> notRemoteConnection
                ? Optional.empty()
                : (finalNoCredential
                    ? Optional.of(new RemoteClusterAliasWithCredentials(remoteClusterAlias, null))
                    : Optional.of(new RemoteClusterAliasWithCredentials(remoteClusterAlias, new SecureString(encodedApiKey.toCharArray()))))
        );

        final AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        final AtomicReference<Authentication> sentAuthentication = new AtomicReference<>();
        final AsyncSender sender = interceptor.interceptSender(new AsyncSender() {
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

    public void testSendWithCrossClusterAccessHeadersThrowsOnOldConnection() throws Exception {
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User(randomAlphaOfLengthBetween(3, 10), randomArray(0, 4, String[]::new, () -> randomAlphaOfLengthBetween(1, 20))))
            .realm()
            .build();
        authentication.writeToContext(threadContext);
        final String remoteClusterAlias = randomAlphaOfLengthBetween(5, 10);
        final String encodedApiKey = randomAlphaOfLengthBetween(10, 42);
        final String remoteClusterCredential = ApiKeyService.withApiKeyPrefix(encodedApiKey);

        final SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mock(SSLService.class),
            securityContext,
            new DestructiveOperations(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
            ),
            mock(CrossClusterAccessAuthenticationService.class),
            mockLicenseState,
            ignored -> Optional.of(new RemoteClusterAliasWithCredentials(remoteClusterAlias, new SecureString(encodedApiKey.toCharArray())))
        );

        final AsyncSender sender = interceptor.interceptSender(new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                fail("sender should not be called");
            }
        });
        final Transport.Connection connection = mock(Transport.Connection.class);
        final TransportVersion versionBeforeCrossClusterAccessRealm = TransportVersionUtils.getPreviousVersion(
            RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY
        );
        final TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.V_7_17_0,
            versionBeforeCrossClusterAccessRealm
        );
        when(connection.getTransportVersion()).thenReturn(version);
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
        assertThat(actualException.get().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            actualException.get().getCause().getMessage(),
            equalTo(
                "Settings for remote cluster ["
                    + remoteClusterAlias
                    + "] indicate cross cluster access headers should be sent but target cluster version ["
                    + connection.getTransportVersion().toReleaseVersion()
                    + "] does not support receiving them"
            )
        );
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
            mock(SSLService.class),
            securityContext,
            new DestructiveOperations(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
            ),
            mock(CrossClusterAccessAuthenticationService.class),
            mockLicenseState,
            ignored -> Optional.of(new RemoteClusterAliasWithCredentials(remoteClusterAlias, new SecureString(encodedApiKey.toCharArray())))
        );

        final AsyncSender sender = interceptor.interceptSender(new AsyncSender() {
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
        final SSLService sslService = mock(SSLService.class);

        when(sslService.getSSLConfiguration("xpack.security.transport.ssl.")).thenReturn(
            new SslConfiguration(
                "xpack.security.transport.ssl",
                randomBoolean(),
                mock(SslTrustConfig.class),
                mock(SslKeyConfig.class),
                randomFrom(SslVerificationMode.values()),
                SslClientAuthenticationMode.REQUIRED,
                List.of("TLS_AES_256_GCM_SHA384"),
                List.of("TLSv1.3")
            )
        );

        when(sslService.getSSLConfiguration("xpack.security.remote_cluster_server.ssl.")).thenReturn(
            new SslConfiguration(
                "xpack.security.remote_cluster_server.ssl",
                randomBoolean(),
                mock(SslTrustConfig.class),
                mock(SslKeyConfig.class),
                randomFrom(SslVerificationMode.values()),
                SslClientAuthenticationMode.NONE,
                List.of("TLS_RSA_WITH_AES_256_GCM_SHA384"),
                List.of("TLSv1.2")
            )
        );
        doThrow(new AssertionError("profile filters should not be configured for remote cluster client")).when(sslService)
            .getSSLConfiguration("xpack.security.remote_cluster_client.ssl.");

        final var securityServerTransportInterceptor = new SecurityServerTransportInterceptor(
            builder.build(),
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            sslService,
            securityContext,
            new DestructiveOperations(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
            ),
            mock(CrossClusterAccessAuthenticationService.class),
            mockLicenseState
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
        final SSLService sslService = mock(SSLService.class);

        when(sslService.getSSLConfiguration("xpack.security.transport.ssl.")).thenReturn(
            new SslConfiguration(
                "xpack.security.transport.ssl",
                randomBoolean(),
                mock(SslTrustConfig.class),
                mock(SslKeyConfig.class),
                randomFrom(SslVerificationMode.values()),
                SslClientAuthenticationMode.REQUIRED,
                List.of("TLS_AES_256_GCM_SHA384"),
                List.of("TLSv1.3")
            )
        );
        doThrow(new AssertionError("profile filters should not be configured for remote cluster server when the port is disabled")).when(
            sslService
        ).getSSLConfiguration("xpack.security.remote_cluster_server.ssl.");
        doThrow(new AssertionError("profile filters should not be configured for remote cluster client")).when(sslService)
            .getSSLConfiguration("xpack.security.remote_cluster_client.ssl.");

        final var securityServerTransportInterceptor = new SecurityServerTransportInterceptor(
            builder.build(),
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            sslService,
            securityContext,
            new DestructiveOperations(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
            ),
            mock(CrossClusterAccessAuthenticationService.class),
            mockLicenseState
        );

        final Map<String, ServerTransportFilter> profileFilters = securityServerTransportInterceptor.getProfileFilters();
        assertThat(profileFilters.keySet(), contains("default"));
        assertThat(profileFilters.get("default").isExtractClientCert(), is(transportSslEnabled));
    }

    private String[] randomRoles() {
        return generateRandomStringArray(3, 10, false, true);
    }

    @SuppressWarnings("unchecked")
    private static Consumer<ThreadContext.StoredContext> anyConsumer() {
        return any(Consumer.class);
    }

}
