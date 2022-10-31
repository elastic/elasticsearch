/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
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
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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
            )
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
        when(connection.getVersion()).thenReturn(Version.CURRENT);
        sender.sendRequest(connection, "indices:foo", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertEquals(user, sendingUser.get());
        assertEquals(user, securityContext.getUser());
        verify(securityContext, never()).executeAsInternalUser(any(User.class), any(Version.class), anyConsumer());
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
            )
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
        when(connection.getVersion()).thenReturn(Version.CURRENT);
        sender.sendRequest(connection, "internal:foo", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertNotEquals(user, sendingUser.get());
        assertEquals(SystemUser.INSTANCE, sendingUser.get());
        assertEquals(user, securityContext.getUser());
        verify(securityContext).executeAsInternalUser(any(User.class), eq(Version.CURRENT), anyConsumer());
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
            )
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
        when(connection.getVersion()).thenReturn(Version.CURRENT);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> sender.sendRequest(connection, "indices:foo", null, null, null)
        );
        assertEquals("there should always be a user when sending a message for action [indices:foo]", e.getMessage());
        assertNull(securityContext.getUser());
        verify(securityContext, never()).executeAsInternalUser(any(User.class), any(Version.class), anyConsumer());
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
            )
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
        final Version connectionVersion = Version.fromId(Version.CURRENT.id + randomIntBetween(100, 100000));
        assertEquals(Version.CURRENT, Version.min(connectionVersion, Version.CURRENT));

        Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getVersion()).thenReturn(connectionVersion);
        sender.sendRequest(connection, "indices:foo[s]", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertEquals(authentication.getEffectiveSubject().getUser(), sendingUser.get());
        assertEquals(authentication.getEffectiveSubject().getUser(), securityContext.getUser());
        assertEquals(Version.CURRENT, authRef.get().getEffectiveSubject().getVersion());
        assertEquals(Version.CURRENT, authentication.getEffectiveSubject().getVersion());
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
            )
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
        final Version connectionVersion = Version.fromId(Version.CURRENT.id - randomIntBetween(100, 100000));
        assertEquals(connectionVersion, Version.min(connectionVersion, Version.CURRENT));

        Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getVersion()).thenReturn(connectionVersion);
        sender.sendRequest(connection, "indices:foo[s]", null, null, null);
        assertTrue(calledWrappedSender.get());
        assertEquals(authentication.getEffectiveSubject().getUser(), sendingUser.get());
        assertEquals(authentication.getEffectiveSubject().getUser(), securityContext.getUser());
        assertEquals(connectionVersion, authRef.get().getEffectiveSubject().getVersion());
        assertEquals(Version.CURRENT, authentication.getEffectiveSubject().getVersion());
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
            )
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
        final Version connectionVersion = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        when(connection.getVersion()).thenReturn(connectionVersion);

        sender.sendRequest(connection, "indices:foo[s]", null, null, null);
        assertThat(calledWrappedSender.get(), is(true));
        final Authentication authentication = authenticationRef.get();
        assertThat(authentication, notNullValue());
        assertThat(authentication.getEffectiveSubject().getUser(), equalTo(originToUserMap.get(origin)));
        assertThat(authentication.getEffectiveSubject().getVersion(), equalTo(connectionVersion));
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

    private String[] randomRoles() {
        return generateRandomStringArray(3, 10, false, true);
    }

    @SuppressWarnings("unchecked")
    private static Consumer<ThreadContext.StoredContext> anyConsumer() {
        return any(Consumer.class);
    }
}
