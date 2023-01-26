/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.TcpTransport;
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
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SecurityProfileUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.After;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_PROFILE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomUniquelyNamedRoleDescriptors;
import static org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor.REMOTE_ACCESS_ACTION_ALLOWLIST;
import static org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor.REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put("path.home", createTempDir()).build();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        threadContext = threadPool.getThreadContext();
        securityContext = spy(new SecurityContext(settings, threadPool.getThreadContext()));
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
            new RemoteClusterAuthorizationResolver(settings, clusterService.getClusterSettings())
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
        when(connection.getTransportVersion()).thenReturn(TransportVersion.CURRENT);
        sender.sendRequest(connection, "indices:foo", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertEquals(user, sendingUser.get());
        assertEquals(user, securityContext.getUser());
        verify(securityContext, never()).executeAsInternalUser(any(User.class), any(TransportVersion.class), anyConsumer());
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
            new RemoteClusterAuthorizationResolver(settings, clusterService.getClusterSettings())
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
        when(connection.getTransportVersion()).thenReturn(TransportVersion.CURRENT);
        sender.sendRequest(connection, "internal:foo", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertNotEquals(user, sendingUser.get());
        assertEquals(SystemUser.INSTANCE, sendingUser.get());
        assertEquals(user, securityContext.getUser());
        verify(securityContext).executeAsInternalUser(any(User.class), eq(TransportVersion.CURRENT), anyConsumer());
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
            new RemoteClusterAuthorizationResolver(settings, clusterService.getClusterSettings())
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
        when(connection.getTransportVersion()).thenReturn(TransportVersion.CURRENT);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> sender.sendRequest(connection, "indices:foo", null, null, null)
        );
        assertEquals("there should always be a user when sending a message for action [indices:foo]", e.getMessage());
        assertNull(securityContext.getUser());
        verify(securityContext, never()).executeAsInternalUser(any(User.class), any(TransportVersion.class), anyConsumer());
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
            new RemoteClusterAuthorizationResolver(settings, clusterService.getClusterSettings())
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
        final TransportVersion connectionVersion = TransportVersion.fromId(Version.CURRENT.id + randomIntBetween(100, 100000));
        assertEquals(TransportVersion.CURRENT, TransportVersion.min(connectionVersion, TransportVersion.CURRENT));

        Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(connectionVersion);
        sender.sendRequest(connection, "indices:foo[s]", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertEquals(authentication.getEffectiveSubject().getUser(), sendingUser.get());
        assertEquals(authentication.getEffectiveSubject().getUser(), securityContext.getUser());
        assertEquals(TransportVersion.CURRENT, authRef.get().getEffectiveSubject().getTransportVersion());
        assertEquals(TransportVersion.CURRENT, authentication.getEffectiveSubject().getTransportVersion());
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
            new RemoteClusterAuthorizationResolver(settings, clusterService.getClusterSettings())
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
        final TransportVersion connectionVersion = TransportVersion.fromId(Version.CURRENT.id - randomIntBetween(100, 100000));
        assertEquals(connectionVersion, TransportVersion.min(connectionVersion, TransportVersion.CURRENT));

        Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(connectionVersion);
        sender.sendRequest(connection, "indices:foo[s]", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertEquals(authentication.getEffectiveSubject().getUser(), sendingUser.get());
        assertEquals(authentication.getEffectiveSubject().getUser(), securityContext.getUser());
        assertEquals(connectionVersion, authRef.get().getEffectiveSubject().getTransportVersion());
        assertEquals(TransportVersion.CURRENT, authentication.getEffectiveSubject().getTransportVersion());
    }

    public void testSetUserBasedOnActionOrigin() {
        final Map<String, User> originToUserMap = Map.of(
            SECURITY_ORIGIN,
            XPackSecurityUser.INSTANCE,
            SECURITY_PROFILE_ORIGIN,
            SecurityProfileUser.INSTANCE,
            TRANSFORM_ORIGIN,
            XPackUser.INSTANCE,
            ASYNC_SEARCH_ORIGIN,
            AsyncSearchUser.INSTANCE
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
            new RemoteClusterAuthorizationResolver(settings, clusterService.getClusterSettings())
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
        final TransportVersion connectionVersion = TransportVersionUtils.randomCompatibleVersion(random(), TransportVersion.CURRENT);
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
                    public void handleResponse(TransportResponse.Empty response) {
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
                    public void handleResponse(TransportResponse.Empty response) {
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
                DeleteIndexAction.NAME,
                randomBoolean(),
                randomBoolean() ? ThreadPool.Names.SAME : ThreadPool.Names.GENERIC,
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

    public void testSendWithRemoteAccessHeaders() throws Exception {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        final boolean internalUser = randomBoolean();
        final Authentication authentication = internalUser
            ? AuthenticationTestHelper.builder().internal(SystemUser.INSTANCE).build()
            : AuthenticationTestHelper.builder().user(new User(randomAlphaOfLengthBetween(3, 10), randomRoles())).realm().build();
        authentication.writeToContext(threadContext);
        final RemoteClusterAuthorizationResolver remoteClusterAuthorizationResolver = mock(RemoteClusterAuthorizationResolver.class);
        final String remoteClusterCredential = randomAlphaOfLengthBetween(10, 42);
        when(remoteClusterAuthorizationResolver.resolveAuthorization(any())).thenReturn(remoteClusterCredential);
        final String remoteClusterAlias = randomAlphaOfLengthBetween(5, 10);
        final AuthorizationService authzService = mock(AuthorizationService.class);
        // We capture the listener so that we can complete the full flow, by calling onResponse further down
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<ActionListener<RoleDescriptorsIntersection>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doAnswer(i -> null).when(authzService).retrieveRemoteAccessRoleDescriptorsIntersection(any(), any(), listenerCaptor.capture());

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
            remoteClusterAuthorizationResolver,
            ignored -> Optional.of(remoteClusterAlias)
        );

        final AtomicBoolean calledWrappedSender = new AtomicBoolean(false);
        final AtomicReference<String> sentCredential = new AtomicReference<>();
        final AtomicReference<RemoteAccessAuthentication> sentRemoteAccessAuthentication = new AtomicReference<>();
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
                assertThat(securityContext.getAuthentication(), nullValue());
                sentCredential.set(securityContext.getThreadContext().getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY));
                try {
                    sentRemoteAccessAuthentication.set(RemoteAccessAuthentication.readFromContext(securityContext.getThreadContext()));
                } catch (IOException e) {
                    fail("no exceptions expected but got " + e);
                }
                handler.handleResponse(null);
            }
        });
        final Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(TransportVersion.CURRENT);
        final Tuple<String, TransportRequest> actionAndReq = randomAllowlistedActionAndRequest();
        sender.sendRequest(connection, actionAndReq.v1(), actionAndReq.v2(), null, new TransportResponseHandler<>() {
            @Override
            public void handleResponse(TransportResponse response) {
                // Headers should get restored before handle response is called
                assertThat(securityContext.getAuthentication(), equalTo(authentication));
                assertThat(securityContext.getThreadContext().getHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY), nullValue());
                assertThat(securityContext.getThreadContext().getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY), nullValue());
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
        final RoleDescriptorsIntersection expectedRoleDescriptorsIntersection;
        if (internalUser) {
            expectedRoleDescriptorsIntersection = RoleDescriptorsIntersection.EMPTY;
        } else {
            expectedRoleDescriptorsIntersection = new RoleDescriptorsIntersection(
                randomList(0, 3, () -> Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 1)))
            );
            // Call listener to complete flow
            listenerCaptor.getValue().onResponse(expectedRoleDescriptorsIntersection);
        }
        assertTrue(calledWrappedSender.get());
        assertThat(sentCredential.get(), equalTo("ApiKey " + remoteClusterCredential));
        assertThat(
            sentRemoteAccessAuthentication.get(),
            equalTo(new RemoteAccessAuthentication(authentication, expectedRoleDescriptorsIntersection))
        );
        verify(securityContext, never()).executeAsInternalUser(any(), any(), anyConsumer());
        verify(remoteClusterAuthorizationResolver, times(1)).resolveAuthorization(eq(remoteClusterAlias));
        verify(authzService, times(internalUser ? 0 : 1)).retrieveRemoteAccessRoleDescriptorsIntersection(
            eq(remoteClusterAlias),
            eq(authentication.getEffectiveSubject()),
            anyActionListener()
        );
        assertThat(securityContext.getThreadContext().getHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY), nullValue());
        assertThat(securityContext.getThreadContext().getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY), nullValue());
    }

    public void testSendWithUserIfRemoteAccessHeadersConditionNotMet() throws Exception {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        boolean noCredential = randomBoolean();
        final boolean notRemoteConnection = randomBoolean();
        final boolean nonAllowlistedRequest = randomBoolean();
        final boolean unsupportedAuthentication = randomBoolean();
        // Ensure at least one condition fails
        if (false == (notRemoteConnection || noCredential || nonAllowlistedRequest || unsupportedAuthentication)) {
            noCredential = true;
        }
        final RemoteClusterAuthorizationResolver remoteClusterAuthorizationResolver = mock(RemoteClusterAuthorizationResolver.class);
        when(remoteClusterAuthorizationResolver.resolveAuthorization(any())).thenReturn(
            noCredential ? null : randomAlphaOfLengthBetween(10, 42)
        );
        final AuthenticationTestHelper.AuthenticationTestBuilder builder = AuthenticationTestHelper.builder();
        final Authentication authentication;
        if (unsupportedAuthentication) {
            authentication = randomFrom(builder.apiKey().build(), builder.serviceAccount().build());
        } else {
            authentication = builder.user(new User(randomAlphaOfLengthBetween(3, 10), randomRoles())).realm().build();
        }
        authentication.writeToContext(threadContext);
        final Tuple<String, TransportRequest> actionAndReq = nonAllowlistedRequest
            ? new Tuple<>(FollowInfoAction.NAME, mock(TransportRequest.class))
            : randomAllowlistedActionAndRequest();

        final String remoteClusterAlias = randomAlphaOfLengthBetween(5, 10);
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
            remoteClusterAuthorizationResolver,
            ignored -> notRemoteConnection ? Optional.empty() : Optional.of(remoteClusterAlias)
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
                assertThat(securityContext.getThreadContext().getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY), nullValue());
                assertThat(securityContext.getThreadContext().getHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY), nullValue());
            }
        });
        final Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(TransportVersion.CURRENT);
        sender.sendRequest(connection, actionAndReq.v1(), actionAndReq.v2(), null, null);
        assertTrue(calledWrappedSender.get());
        assertThat(sentAuthentication.get(), equalTo(authentication));
        verify(authzService, never()).retrieveRemoteAccessRoleDescriptorsIntersection(any(), any(), anyActionListener());
        assertThat(securityContext.getThreadContext().getHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY), nullValue());
        assertThat(securityContext.getThreadContext().getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY), nullValue());
    }

    public void testSendWithRemoteAccessHeadersThrowsOnOldConnection() throws Exception {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(
                new User(
                    randomAlphaOfLengthBetween(3, 10),
                    randomArray(
                        0,
                        4,
                        String[]::new,
                        () -> randomValueOtherThanMany(ReservedRolesStore::isReserved, () -> randomAlphaOfLengthBetween(1, 20))
                    )
                )
            )
            .realm()
            .build();
        authentication.writeToContext(threadContext);
        final RemoteClusterAuthorizationResolver remoteClusterAuthorizationResolver = mock(RemoteClusterAuthorizationResolver.class);
        final String remoteClusterCredential = randomAlphaOfLengthBetween(10, 42);
        when(remoteClusterAuthorizationResolver.resolveAuthorization(any())).thenReturn(remoteClusterCredential);
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
            remoteClusterAuthorizationResolver,
            ignored -> Optional.of(remoteClusterAlias)
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
        final TransportVersion versionBeforeRemoteAccessHeaders = TransportVersionUtils.getPreviousVersion(TransportVersion.V_8_7_0);
        final TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            versionBeforeRemoteAccessHeaders.calculateMinimumCompatVersion(),
            versionBeforeRemoteAccessHeaders
        );
        when(connection.getTransportVersion()).thenReturn(version);
        final Tuple<String, TransportRequest> actionAndReq = randomAllowlistedActionAndRequest();
        final AtomicBoolean calledHandleException = new AtomicBoolean(false);
        final AtomicReference<TransportException> actualException = new AtomicReference<>();
        sender.sendRequest(connection, actionAndReq.v1(), actionAndReq.v2(), null, new TransportResponseHandler<>() {
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
                    + "] indicate remote access headers should be sent but target cluster version ["
                    + connection.getTransportVersion()
                    + "] does not support receiving them"
            )
        );
        verify(remoteClusterAuthorizationResolver, times(1)).resolveAuthorization(eq(remoteClusterAlias));
        assertThat(securityContext.getThreadContext().getHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY), nullValue());
        assertThat(securityContext.getThreadContext().getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY), nullValue());
    }

    private Tuple<String, TransportRequest> randomAllowlistedActionAndRequest() {
        final String action = randomFrom(REMOTE_ACCESS_ACTION_ALLOWLIST.toArray(new String[0]));
        return new Tuple<>(action, mock(TransportRequest.class));
    }

    private String[] randomRoles() {
        return generateRandomStringArray(3, 10, false, true);
    }

    @SuppressWarnings("unchecked")
    private static Consumer<ThreadContext.StoredContext> anyConsumer() {
        return any(Consumer.class);
    }
}
