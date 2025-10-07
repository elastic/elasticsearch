/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse.Empty;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor.AsyncSender;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SslProfile;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.After;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.DATA_STREAM_LIFECYCLE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_PROFILE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SecurityServerTransportInterceptorTests extends AbstractServerTransportInterceptorTests {

    private Settings settings;
    private ThreadPool threadPool;
    private ThreadContext threadContext;
    private SecurityContext securityContext;
    private ClusterService clusterService;
    private DestructiveOperations destructiveOperations;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put("path.home", createTempDir()).build();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        threadContext = threadPool.getThreadContext();
        securityContext = spy(new SecurityContext(settings, threadPool.getThreadContext()));
        destructiveOperations = new DestructiveOperations(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, Collections.singleton(DestructiveOperations.REQUIRES_NAME_SETTING))
        );
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
            mockSslService(),
            securityContext,
            destructiveOperations,
            new TestNoopRemoteClusterTransportInterceptor()
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
        AuthorizationServiceField.ORIGINATING_ACTION_VALUE.set(threadContext, "indices:foo");
        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mockSslService(),
            securityContext,
            destructiveOperations,
            new TestNoopRemoteClusterTransportInterceptor()
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
            mockSslService(),
            securityContext,
            destructiveOperations,
            new TestNoopRemoteClusterTransportInterceptor()
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
        AuthorizationServiceField.ORIGINATING_ACTION_VALUE.set(threadContext, "indices:foo");

        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mockSslService(),
            securityContext,
            destructiveOperations,
            new TestNoopRemoteClusterTransportInterceptor()
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
        AuthorizationServiceField.ORIGINATING_ACTION_VALUE.set(threadContext, "indices:foo");

        SecurityServerTransportInterceptor interceptor = new SecurityServerTransportInterceptor(
            settings,
            threadPool,
            mock(AuthenticationService.class),
            mock(AuthorizationService.class),
            mockSslService(),
            securityContext,
            destructiveOperations,
            new TestNoopRemoteClusterTransportInterceptor()
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
            mockSslService(),
            securityContext,
            destructiveOperations,
            new TestNoopRemoteClusterTransportInterceptor()
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

    private static class TestNoopRemoteClusterTransportInterceptor implements RemoteClusterTransportInterceptor {

        @Override
        public AsyncSender interceptSender(AsyncSender sender) {
            return sender;
        }

        @Override
        public boolean isRemoteClusterConnection(Connection connection) {
            return false;
        }

        @Override
        public Optional<ServerTransportFilter> getRemoteProfileTransportFilter(
            SslProfile sslProfile,
            DestructiveOperations destructiveOperations
        ) {
            return Optional.empty();
        }

        @Override
        public boolean hasRemoteClusterAccessHeadersInContext(SecurityContext securityContext) {
            return false;
        }
    }

}
